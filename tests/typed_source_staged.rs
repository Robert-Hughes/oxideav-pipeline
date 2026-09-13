//! Staged-runner typed-source integration tests.
//!
//! Historically the pipelined (stage-per-thread) runner could only
//! drive bytes-shape sources: `Executor::run` silently fell back to
//! the serial path when any source resolved to `SourceOutput::Packets`
//! / `SourceOutput::Frames`, and `Executor::spawn` — the playback
//! path, which has no serial fallback — failed outright with an
//! internal-sounding "opener returned non-bytes shape" error. That
//! meant no live handle (seek / progress / abort) over RTMP-style
//! packet sources or generator-style frame sources.
//!
//! These tests pin the staged runner's native typed-source support:
//!
//! * `spawn()` over a frame-shape source delivers every frame and
//!   reports serial-parity stats.
//! * `spawn()` over a packet-shape source runs the decode chain and
//!   reports serial-parity stats.
//! * A seek dispatched against a typed source (no seek surface on
//!   `PacketSource` / `FrameSource`) surfaces a
//!   `BarrierKind::SeekRejected` with the dispatch's generation, and
//!   payloads keep flowing — the pipeline must not die.
//! * `run()` stats for typed sources are identical between the serial
//!   (`threads = 1`) and pipelined (`threads = 2`) paths.

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{
    packet::PacketFlags, AudioFrame, CancellationToken, CodecCapabilities, CodecId,
    CodecParameters, Decoder, DecoderFactory, Error, Frame, FrameLease, FrameSource, MediaType,
    Packet, PacketSource, Result, RuntimeContext, SampleFormat, StreamInfo, TimeBase,
};
use oxideav_core::{registry::CodecInfo, CodecRegistry};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink, TrackSink, TrackSinkInfo};

const CODEC: &str = "typed_staged_pcm";
const SAMPLE_RATE: u32 = 8_000;
/// Frames / packets emitted by the finite sources.
const FINITE_LEN: u64 = 25;

fn audio_params() -> CodecParameters {
    let mut p = CodecParameters::audio(CodecId::new(CODEC));
    p.sample_rate = Some(SAMPLE_RATE);
    p.channels = Some(1);
    p.sample_format = Some(SampleFormat::S16);
    p
}

fn audio_stream() -> StreamInfo {
    StreamInfo {
        index: 0,
        time_base: TimeBase::new(1, SAMPLE_RATE as i64),
        duration: None,
        start_time: Some(0),
        params: audio_params(),
    }
}

// ───────────────────────── mock sources ─────────────────────────

/// Frame source emitting `remaining` 16-sample audio frames. Pass
/// `u64::MAX` for an endless source (seek tests need the pipeline to
/// outlive the seek dispatch).
struct MockFrameSource {
    params: CodecParameters,
    pts: i64,
    remaining: u64,
}

impl FrameSource for MockFrameSource {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if self.remaining == 0 {
            return Err(Error::Eof);
        }
        self.remaining -= 1;
        let pts = self.pts;
        self.pts += 16;
        Ok(Frame::Audio(AudioFrame {
            samples: 16,
            pts: Some(pts),
            data: vec![vec![0u8; 32]],
        }))
    }
}

struct MockPacketSource {
    streams: Vec<StreamInfo>,
    pts: i64,
    remaining: u64,
}

impl PacketSource for MockPacketSource {
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        if self.remaining == 0 {
            return Err(Error::Eof);
        }
        self.remaining -= 1;
        let pts = self.pts;
        self.pts += 16;
        Ok(Packet {
            stream_index: 0,
            time_base: TimeBase::new(1, SAMPLE_RATE as i64),
            pts: Some(pts),
            dts: Some(pts),
            duration: Some(16),
            flags: PacketFlags::default(),
            data: vec![0u8; 32],
        })
    }
}

/// 1:1 packet → AudioFrame passthrough decoder for the packet path.
struct PassthroughDecoder {
    pending: Option<Packet>,
}

impl Decoder for PassthroughDecoder {
    fn codec_id(&self) -> &CodecId {
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(CODEC))
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        match self.pending.take() {
            Some(p) => Ok(Frame::Audio(AudioFrame {
                samples: (p.data.len() / 2) as u32,
                pts: p.pts,
                data: vec![p.data],
            })),
            None => Err(Error::NeedMore),
        }
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
    Ok(Box::new(PassthroughDecoder { pending: None }))
}

fn register_codec(codecs: &mut CodecRegistry) {
    let info = CodecInfo::new(CodecId::new(CODEC))
        .capabilities(CodecCapabilities::audio(CODEC).with_decode())
        .decoder(make_decoder as DecoderFactory);
    codecs.register(info);
}

/// Frame-shape URIs: `tsfin://` emits FINITE_LEN frames; `tsinf://`
/// never ends (EOF only via abort). Packet-shape: `tspkt://` emits
/// FINITE_LEN packets.
fn make_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    register_codec(&mut ctx.codecs);
    ctx.sources.register_frames("tsfin", |_uri| {
        Ok(Box::new(MockFrameSource {
            params: audio_params(),
            pts: 0,
            remaining: FINITE_LEN,
        }))
    });
    ctx.sources.register_frames("tsinf", |_uri| {
        Ok(Box::new(MockFrameSource {
            params: audio_params(),
            pts: 0,
            remaining: u64::MAX,
        }))
    });
    ctx.sources.register_packets("tspkt", |_uri| {
        Ok(Box::new(MockPacketSource {
            streams: vec![audio_stream()],
            pts: 0,
            remaining: FINITE_LEN,
        }))
    });
    ctx
}

// ───────────────────────── observer sink ─────────────────────────

enum SinkEvent {
    Started,
    Payload,
    Barrier(BarrierKind),
    Finished,
}

struct ChannelSink {
    tx: SyncSender<SinkEvent>,
    frames: Arc<Mutex<u64>>,
}

impl JobSink for ChannelSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Started);
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Payload);
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        *self.frames.lock().unwrap() += 1;
        let _ = self.tx.send(SinkEvent::Payload);
        Ok(())
    }
    fn barrier(&mut self, kind: BarrierKind) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Barrier(kind));
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Finished);
        Ok(())
    }
}

struct BlockingTrackSink {
    cancellation: CancellationToken,
}

impl TrackSink for BlockingTrackSink {
    fn write_packet(
        &mut self,
        _stream_index: u32,
        _kind: MediaType,
        _packet: Packet,
    ) -> Result<()> {
        Err(Error::unsupported("test sink expects decoded frames"))
    }

    fn write_frame_lease(
        &mut self,
        _stream_index: u32,
        _kind: MediaType,
        _frame: FrameLease,
    ) -> Result<()> {
        while !self.cancellation.is_cancelled() {
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(Error::cancelled("test TrackSink cancelled"))
    }
}

struct ObservingTrackSink {
    tx: SyncSender<u32>,
}

impl TrackSink for ObservingTrackSink {
    fn write_packet(
        &mut self,
        _stream_index: u32,
        _kind: MediaType,
        _packet: Packet,
    ) -> Result<()> {
        Err(Error::unsupported("test sink expects decoded frames"))
    }

    fn write_frame_lease(
        &mut self,
        stream_index: u32,
        _kind: MediaType,
        _frame: FrameLease,
    ) -> Result<()> {
        self.tx
            .send(stream_index)
            .map_err(|_| Error::other("observer receiver dropped"))
    }
}

struct IndependentTrackJobSink {
    observed_tx: SyncSender<u32>,
}

impl JobSink for IndependentTrackJobSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }

    fn open_track_sinks(
        &mut self,
        tracks: &[TrackSinkInfo],
        cancellation: CancellationToken,
    ) -> Result<Option<Vec<Box<dyn TrackSink + Send>>>> {
        assert_eq!(tracks.len(), 2);
        Ok(Some(vec![
            Box::new(BlockingTrackSink {
                cancellation: cancellation.clone(),
            }),
            Box::new(ObservingTrackSink {
                tx: self.observed_tx.clone(),
            }),
        ]))
    }

    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Err(Error::other(
            "aggregate JobSink packet callback must not be used in TrackSink mode",
        ))
    }

    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        Err(Error::other(
            "aggregate JobSink frame callback must not be used in TrackSink mode",
        ))
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn dual_display_job(uri: &str) -> Job {
    let job_json =
        format!(r#"{{"@display": {{"audio": [{{"from": "{uri}"}}, {{"from": "{uri}"}}]}}}}"#);
    Job::from_json(&job_json).expect("parse dual-track job")
}

fn display_job(uri: &str) -> Job {
    let job_json = format!(r#"{{"@display": {{"audio": [{{"from": "{uri}"}}]}}}}"#);
    Job::from_json(&job_json).expect("parse job")
}

fn wait_for<F>(rx: &Receiver<SinkEvent>, deadline: Instant, mut pred: F) -> Option<SinkEvent>
where
    F: FnMut(&SinkEvent) -> bool,
{
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining.min(Duration::from_millis(100))) {
            Ok(ev) => {
                if pred(&ev) {
                    return Some(ev);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return None,
        }
    }
    None
}

// ───────────────────────── tests ─────────────────────────

#[test]
fn spawn_frame_source_delivers_all_frames() {
    // Pre-fix: spawn() over a frame-shape source failed at prep with
    // "opener returned non-bytes shape; use open_source() instead".
    let ctx = make_ctx();
    let job = display_job("tsfin://gen");
    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(256);
    let frames = Arc::new(Mutex::new(0u64));
    let sink = Box::new(ChannelSink {
        tx,
        frames: frames.clone(),
    });

    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", sink)
        .spawn()
        .expect("spawn over a frame-shape source must succeed");

    let deadline = Instant::now() + Duration::from_secs(10);
    wait_for(&rx, deadline, |e| matches!(e, SinkEvent::Finished))
        .expect("Finished event never arrived");
    let stats = handle.stop().expect("stop");

    assert_eq!(*frames.lock().unwrap(), FINITE_LEN);
    assert_eq!(
        stats.frames_decoded, FINITE_LEN,
        "one frames_decoded tick per source frame (serial parity)"
    );
    assert_eq!(stats.frames_written, FINITE_LEN);
    assert_eq!(stats.packets_read, 0, "no packets exist on the frame path");
}

#[test]
fn spawn_packet_source_runs_decode_chain() {
    // Pre-fix: spawn() over a packet-shape source failed the same way.
    let ctx = make_ctx();
    let job = display_job("tspkt://live");
    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(256);
    let frames = Arc::new(Mutex::new(0u64));
    let sink = Box::new(ChannelSink {
        tx,
        frames: frames.clone(),
    });

    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", sink)
        .spawn()
        .expect("spawn over a packet-shape source must succeed");

    let deadline = Instant::now() + Duration::from_secs(10);
    wait_for(&rx, deadline, |e| matches!(e, SinkEvent::Finished))
        .expect("Finished event never arrived");
    let stats = handle.stop().expect("stop");

    assert_eq!(*frames.lock().unwrap(), FINITE_LEN);
    assert_eq!(stats.packets_read, FINITE_LEN);
    assert_eq!(stats.frames_decoded, FINITE_LEN);
    assert_eq!(stats.frames_written, FINITE_LEN);
}

#[test]
fn independent_track_sinks_keep_sibling_track_moving_under_backpressure() {
    let ctx = make_ctx();
    let job = dual_display_job("tspkt://live");
    let (observed_tx, observed_rx) = mpsc::sync_channel::<u32>(32);
    let sink = Box::new(IndependentTrackJobSink { observed_tx });

    let handle = Executor::new(&job, &ctx)
        .with_threads(4)
        .with_sink_override("@display", sink)
        .spawn()
        .expect("spawn dual-track packet source");

    let observed_track = observed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second TrackSink made no progress while first TrackSink was blocked");
    assert_eq!(observed_track, 1);

    handle.request_abort();
    let stats = handle
        .stop()
        .expect("cancelling a blocked TrackSink should stop cleanly");
    assert!(
        stats.frames_written >= 1,
        "successful direct TrackSink delivery must count as frames_written"
    );
}

#[test]
fn seek_on_frame_source_rejected_with_matching_generation_and_stream_survives() {
    // FrameSource has no seek surface — the dispatch must surface a
    // SeekRejected barrier carrying the handle-assigned generation,
    // and frames must keep flowing afterwards (the pre-existing
    // demuxer contract, extended to typed sources).
    let ctx = make_ctx();
    let job = display_job("tsinf://endless");
    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(256);
    let frames = Arc::new(Mutex::new(0u64));
    let sink = Box::new(ChannelSink {
        tx,
        frames: frames.clone(),
    });

    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", sink)
        .spawn()
        .expect("spawn endless frame source");

    let deadline = Instant::now() + Duration::from_secs(10);
    wait_for(&rx, deadline, |e| matches!(e, SinkEvent::Payload)).expect("first payload");

    let generation = handle
        .seek_with_generation(0, 1_000, TimeBase::new(1, SAMPLE_RATE as i64))
        .expect("seek dispatch");

    let barrier = wait_for(&rx, deadline, |e| matches!(e, SinkEvent::Barrier(_)))
        .expect("SeekRejected barrier never arrived");
    match barrier {
        SinkEvent::Barrier(BarrierKind::SeekRejected { generation: g }) => {
            assert_eq!(g, generation, "barrier must carry the dispatch generation");
        }
        SinkEvent::Barrier(other) => panic!("expected SeekRejected, got {other:?}"),
        _ => unreachable!(),
    }

    // Payloads continue after the rejection — the pipeline is alive.
    wait_for(&rx, deadline, |e| matches!(e, SinkEvent::Payload))
        .expect("stream died after rejected seek");

    let stats = handle.stop().expect("stop");
    assert!(stats.frames_written > 0);
}

/// `run()` over typed sources must produce identical stats on the
/// serial and pipelined paths — the pipelined runner drives the same
/// chains natively now instead of falling back.
#[test]
fn typed_source_run_stats_parity_serial_vs_pipelined() {
    for uri in ["tsfin://gen", "tspkt://live"] {
        let mut per_thread_stats = Vec::new();
        for threads in [1usize, 2] {
            let ctx = make_ctx();
            let job = display_job(uri);
            let (tx, _rx) = mpsc::sync_channel::<SinkEvent>(4096);
            let frames = Arc::new(Mutex::new(0u64));
            let sink = Box::new(ChannelSink {
                tx,
                frames: frames.clone(),
            });
            let stats = Executor::new(&job, &ctx)
                .with_threads(threads)
                .with_sink_override("@display", sink)
                .run()
                .expect("run");
            per_thread_stats.push((
                stats.packets_read,
                stats.frames_decoded,
                stats.frames_written,
                *frames.lock().unwrap(),
            ));
        }
        assert_eq!(
            per_thread_stats[0], per_thread_stats[1],
            "{uri}: serial vs pipelined stats diverged"
        );
    }
}
