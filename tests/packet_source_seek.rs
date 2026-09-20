//! PacketSource seek integration: a packet-shaped source can reposition itself,
//! surface SeekFlush through the public executor handle, and continue producing
//! post-seek packets without a container-demux stage.

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use oxideav_core::{
    registry::CodecInfo, AudioFrame, CodecCapabilities, CodecId, CodecParameters, Decoder,
    DecoderFactory, Error, Frame, MediaType, Packet, PacketSource, Result, RuntimeContext,
    SampleFormat, StreamInfo, TimeBase,
};
use oxideav_pipeline::{BarrierKind, EofMode, Executor, Job, JobSink};

const CODEC: &str = "packet_seek_pcm";
const RATE: i64 = 1_000;
const PACKET_SAMPLES: i64 = 100;
const PACKETS: i64 = 600;

struct SeekablePackets {
    streams: Vec<StreamInfo>,
    next: i64,
}

impl SeekablePackets {
    fn new() -> Self {
        let mut params = CodecParameters::audio(CodecId::new(CODEC));
        params.sample_rate = Some(RATE as u32);
        params.channels = Some(1);
        params.sample_format = Some(SampleFormat::S16);
        Self {
            streams: vec![StreamInfo {
                index: 0,
                time_base: TimeBase::new(1, RATE),
                duration: Some(PACKETS * PACKET_SAMPLES),
                start_time: Some(0),
                params,
            }],
            next: 0,
        }
    }
}

impl PacketSource for SeekablePackets {
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }

    fn next_packet(&mut self) -> Result<Packet> {
        if self.next >= PACKETS {
            return Err(Error::Eof);
        }
        let pts = self.next * PACKET_SAMPLES;
        self.next += 1;
        let mut packet = Packet::new(
            0,
            TimeBase::new(1, RATE),
            vec![0; (PACKET_SAMPLES * 2) as usize],
        );
        packet.pts = Some(pts);
        packet.duration = Some(PACKET_SAMPLES);
        Ok(packet)
    }

    fn seek_to(&mut self, stream_index: u32, pts: i64) -> Result<i64> {
        if stream_index != 0 {
            return Err(Error::invalid("packet seek test has only stream 0"));
        }
        let clamped = pts.clamp(0, PACKETS * PACKET_SAMPLES);
        self.next = clamped / PACKET_SAMPLES;
        Ok(self.next * PACKET_SAMPLES)
    }

    fn supports_seek(&self) -> bool {
        true
    }
}

fn open_packets(_uri: &str) -> Result<Box<dyn PacketSource>> {
    Ok(Box::new(SeekablePackets::new()))
}

struct NonSeekPackets(SeekablePackets);

impl PacketSource for NonSeekPackets {
    fn streams(&self) -> &[StreamInfo] {
        self.0.streams()
    }

    fn next_packet(&mut self) -> Result<Packet> {
        self.0.next_packet()
    }
}

fn open_nonseek_packets(_uri: &str) -> Result<Box<dyn PacketSource>> {
    Ok(Box::new(NonSeekPackets(SeekablePackets::new())))
}

struct PassDecoder {
    pending: Option<Packet>,
}

impl Decoder for PassDecoder {
    fn codec_id(&self) -> &CodecId {
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(CODEC))
    }

    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }

    fn receive_frame(&mut self) -> Result<Frame> {
        let packet = self.pending.take().ok_or(Error::NeedMore)?;
        Ok(Frame::Audio(AudioFrame {
            samples: PACKET_SAMPLES as u32,
            pts: packet.pts,
            data: vec![packet.data],
        }))
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.pending = None;
        Ok(())
    }
}

fn make_decoder(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(PassDecoder { pending: None }))
}

fn register(ctx: &mut RuntimeContext) {
    ctx.sources.register_packets("seekpack", open_packets);
    ctx.sources
        .register_packets("noseekpack", open_nonseek_packets);
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(CODEC))
            .capabilities(CodecCapabilities::audio(CODEC).with_decode())
            .decoder(make_decoder as DecoderFactory),
    );
}

enum Event {
    Started(Vec<StreamInfo>),
    Frame(Option<i64>),
    Barrier(BarrierKind),
    EndOfStream(MediaType),
    Finished,
}

struct Sink {
    tx: SyncSender<Event>,
}

impl JobSink for Sink {
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()> {
        self.tx.send(Event::Started(streams.to_vec())).unwrap();
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, frame: &Frame) -> Result<()> {
        let pts = match frame {
            Frame::Audio(frame) => frame.pts,
            _ => None,
        };
        self.tx.send(Event::Frame(pts)).unwrap();
        Ok(())
    }
    fn barrier(&mut self, barrier: BarrierKind) -> Result<()> {
        self.tx.send(Event::Barrier(barrier)).unwrap();
        Ok(())
    }
    fn end_of_stream(&mut self, _stream_index: u32, kind: MediaType) -> Result<()> {
        self.tx.send(Event::EndOfStream(kind)).unwrap();
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let _ = self.tx.send(Event::Finished);
        Ok(())
    }
}

fn wait_for<F>(rx: &Receiver<Event>, deadline: Instant, mut pred: F) -> Event
where
    F: FnMut(&Event) -> bool,
{
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Ok(event) = rx.recv_timeout(remaining.min(Duration::from_millis(100))) {
            if pred(&event) {
                return event;
            }
            assert!(!matches!(event, Event::Finished), "executor finished early");
        }
    }
    panic!("timed out waiting for event")
}

#[test]
fn seekable_packet_source_emits_flush_and_continues_at_landing() {
    let mut ctx = RuntimeContext::new();
    register(&mut ctx);
    let job = Job::from_json(
        r#"{
            "@in": {"all": [{"from": "seekpack://fixture"}]},
            "@display": {"audio": [{"from": "@in"}]}
        }"#,
    )
    .unwrap();
    let (tx, rx) = mpsc::sync_channel(8);
    let handle = Executor::new(&job, &ctx)
        .with_sink_override("@display", Box::new(Sink { tx }))
        .with_threads(2)
        .spawn()
        .unwrap();

    let Event::Started(streams) = wait_for(&rx, Instant::now() + Duration::from_secs(2), |event| {
        matches!(event, Event::Started(_))
    }) else {
        unreachable!();
    };
    let stream = &streams[0];
    assert_eq!(stream.duration, Some(PACKETS * PACKET_SAMPLES));

    let _ = wait_for(&rx, Instant::now() + Duration::from_secs(2), |event| {
        matches!(event, Event::Frame(_))
    });

    let target = 30 * RATE;
    let generation = handle
        .seek_with_generation(stream.index, target, stream.time_base)
        .unwrap();
    let Event::Barrier(BarrierKind::SeekFlush {
        generation: got_generation,
        landed_pts,
        time_base,
    }) = wait_for(&rx, Instant::now() + Duration::from_secs(2), |event| {
        matches!(event, Event::Barrier(_))
    })
    else {
        panic!("expected SeekFlush")
    };
    assert_eq!(got_generation, generation);
    assert_eq!(landed_pts, target);
    assert_eq!(time_base, stream.time_base);

    let Event::Frame(Some(pts)) = wait_for(
        &rx,
        Instant::now() + Duration::from_secs(2),
        |event| matches!(event, Event::Frame(Some(pts)) if *pts >= target),
    ) else {
        panic!("expected post-seek frame")
    };
    assert!(pts >= target);
    assert!(pts < target + RATE);

    let drainer = std::thread::spawn(
        move || {
            while rx.recv_timeout(Duration::from_millis(500)).is_ok() {}
        },
    );
    handle.stop().unwrap();
    let _ = drainer.join();
}

#[test]
fn wait_for_seek_parks_at_eof_and_resumes_without_rebuilding() {
    let mut ctx = RuntimeContext::new();
    register(&mut ctx);
    let job = Job::from_json(
        r#"{
            "@in": {"all": [{"from": "seekpack://fixture"}]},
            "@display": {"audio": [{"from": "@in"}]}
        }"#,
    )
    .unwrap();
    let (tx, rx) = mpsc::sync_channel(8);
    let handle = Executor::new(&job, &ctx)
        .with_sink_override("@display", Box::new(Sink { tx }))
        .with_eof_mode(EofMode::WaitForSeek)
        .with_threads(2)
        .spawn()
        .unwrap();

    let Event::Started(streams) = wait_for(&rx, Instant::now() + Duration::from_secs(2), |event| {
        matches!(event, Event::Started(_))
    }) else {
        unreachable!();
    };
    let stream = streams[0].clone();

    let Event::EndOfStream(MediaType::Audio) =
        wait_for(&rx, Instant::now() + Duration::from_secs(2), |event| {
            matches!(event, Event::EndOfStream(MediaType::Audio))
        })
    else {
        panic!("expected retained end-of-stream")
    };
    assert!(
        !handle.has_finished(),
        "WaitForSeek executor must remain alive at EOF"
    );

    let target = 10 * RATE;
    let generation = handle
        .seek_with_generation(stream.index, target, stream.time_base)
        .unwrap();
    let Event::Barrier(BarrierKind::SeekFlush {
        generation: got_generation,
        landed_pts,
        ..
    }) = wait_for(&rx, Instant::now() + Duration::from_secs(2), |event| {
        matches!(event, Event::Barrier(_))
    })
    else {
        panic!("expected SeekFlush after retained EOF")
    };
    assert_eq!(got_generation, generation);
    assert_eq!(landed_pts, target);

    let Event::Frame(Some(pts)) = wait_for(
        &rx,
        Instant::now() + Duration::from_secs(2),
        |event| matches!(event, Event::Frame(Some(pts)) if *pts >= target),
    ) else {
        panic!("expected frame after seek from retained EOF")
    };
    assert!(pts >= target);
    assert!(
        !handle.has_finished(),
        "executor must still be live after resumed seek"
    );

    let drainer = std::thread::spawn(
        move || {
            while rx.recv_timeout(Duration::from_millis(500)).is_ok() {}
        },
    );
    handle.stop().unwrap();
    let _ = drainer.join();
}

#[test]
fn wait_for_seek_rejects_non_seekable_source_during_spawn() {
    let mut ctx = RuntimeContext::new();
    register(&mut ctx);
    let job = Job::from_json(
        r#"{
            "@in": {"all": [{"from": "noseekpack://fixture"}]},
            "@display": {"audio": [{"from": "@in"}]}
        }"#,
    )
    .unwrap();
    let (tx, _rx) = mpsc::sync_channel(8);
    let result = Executor::new(&job, &ctx)
        .with_sink_override("@display", Box::new(Sink { tx }))
        .with_eof_mode(EofMode::WaitForSeek)
        .with_threads(2)
        .spawn();
    let Err(error) = result else {
        panic!("WaitForSeek unexpectedly accepted a non-seekable source")
    };
    assert!(error
        .to_string()
        .contains("requires every routed source to support seeking"));
}

#[test]
fn wait_for_seek_rejects_encoder_graph_before_codec_instantiation() {
    let mut ctx = RuntimeContext::new();
    register(&mut ctx);
    let job = Job::from_json(
        r#"{
            "@in": {"all": [{"from": "seekpack://fixture"}]},
            "@display": {"audio": [{"from": "@in", "codec": "not_registered_on_purpose"}]}
        }"#,
    )
    .unwrap();
    let (tx, _rx) = mpsc::sync_channel(8);
    let result = Executor::new(&job, &ctx)
        .with_sink_override("@display", Box::new(Sink { tx }))
        .with_eof_mode(EofMode::WaitForSeek)
        .with_threads(2)
        .spawn();
    let Err(error) = result else {
        panic!("WaitForSeek unexpectedly accepted an encoder graph")
    };
    assert!(error.to_string().contains("graphs containing encoders"));
}
