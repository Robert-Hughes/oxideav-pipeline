//! Failure-attribution contract tests.
//!
//! `Executor::run_reporting` (and `ExecutorHandle::stop_reporting`)
//! return a [`RunFailure`] that pairs the ORIGINAL first error with
//! *where* it fired: the owning output key, the [`FailureStage`], and
//! the track index when the failing stage belongs to one. Pinned here,
//! each on both executor paths where both exist:
//!
//! * job-level validation failure → `output: None`, `Prepare`
//! * unknown output codec → `Prepare` with the output key
//! * source failure mid-stream → `Source`, no track
//! * filter failure mid-stream → `Filter` with the track index
//! * encoder failure mid-stream and at EOF flush → `Encode` with track
//! * sink `start` / `write` failure → `Sink`
//! * sink `finish` (trailer) failure → `SinkFinish`
//! * multi-output: the failure names the earliest failing output in
//!   document order
//! * `run()` and `run_reporting()` observe the exact same root cause
//! * `stop_reporting()` attributes spawn-path failures identically

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxideav_core::{
    registry::CodecInfo, AudioFrame, CancellationToken, CodecCapabilities, CodecId,
    CodecParameters, Encoder, EncoderFactory, Error, FilterContext, Frame, FrameLease, FrameSource,
    MediaType, Packet, PortSpec, Result, RuntimeContext, SampleFormat, StreamFilter, StreamInfo,
};
use oxideav_pipeline::{
    Executor, FailureStage, Job, JobSink, RunFailure, TrackSink, TrackSinkInfo,
};

const FILTER_ERR: &str = "attribution test filter deliberately failed";
const ENC_SEND_ERR: &str = "attribution test encoder send deliberately failed";
const ENC_FLUSH_ERR: &str = "attribution test encoder flush deliberately failed";
const SINK_WRITE_ERR: &str = "attribution test sink write deliberately failed";
const SINK_START_ERR: &str = "attribution test sink start deliberately failed";
const SINK_FINISH_ERR: &str = "attribution test sink finish deliberately failed";
const SOURCE_ERR: &str = "attribution test source deliberately failed";

// ───────────────────────── failing stages ─────────────────────────

/// Audio pass-through filter that errors on the `fail_on`-th push
/// (1-based).
struct FailNthFilter {
    inp: Vec<PortSpec>,
    outp: Vec<PortSpec>,
    pushes: u64,
    fail_on: u64,
}

impl StreamFilter for FailNthFilter {
    fn input_ports(&self) -> &[PortSpec] {
        &self.inp
    }
    fn output_ports(&self) -> &[PortSpec] {
        &self.outp
    }
    fn push(&mut self, ctx: &mut dyn FilterContext, _port: usize, frame: &Frame) -> Result<()> {
        self.pushes += 1;
        if self.pushes >= self.fail_on {
            return Err(Error::invalid(FILTER_ERR));
        }
        ctx.emit(0, frame.clone())
    }
}

/// Encoder that errors on the `FAIL_ENC_AFTER`-th `send_frame`.
const FAIL_ENC_AFTER: u64 = 3;
const FAIL_ENC_ID: &str = "attr_fail_enc";
/// Encoder that encodes everything but errors at `flush()`.
const FAIL_FLUSH_ENC_ID: &str = "attr_fail_flush_enc";

struct FailingEncoder {
    out_params: CodecParameters,
    sent: u64,
    fail_send_after: Option<u64>,
    fail_flush: bool,
    queue: std::collections::VecDeque<Packet>,
}

impl Encoder for FailingEncoder {
    fn codec_id(&self) -> &CodecId {
        &self.out_params.codec_id
    }
    fn output_params(&self) -> &CodecParameters {
        &self.out_params
    }
    fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.sent += 1;
        if let Some(n) = self.fail_send_after {
            if self.sent > n {
                return Err(Error::invalid(ENC_SEND_ERR));
            }
        }
        let Frame::Audio(a) = frame else {
            return Err(Error::invalid("attribution encoder: audio only"));
        };
        let sr = self.out_params.sample_rate.unwrap_or(8_000);
        let mut pkt = Packet::new(
            0,
            oxideav_core::TimeBase::new(1, sr as i64),
            a.data.first().cloned().unwrap_or_default(),
        );
        pkt.pts = a.pts;
        pkt.dts = a.pts;
        pkt.duration = Some(a.samples as i64);
        pkt.flags.keyframe = true;
        self.queue.push_back(pkt);
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        self.queue.pop_front().ok_or(Error::NeedMore)
    }
    fn flush(&mut self) -> Result<()> {
        if self.fail_flush {
            return Err(Error::invalid(ENC_FLUSH_ERR));
        }
        Ok(())
    }
}

fn make_fail_send_encoder(params: &CodecParameters) -> Result<Box<dyn Encoder>> {
    let mut out = params.clone();
    out.codec_id = CodecId::new(FAIL_ENC_ID);
    Ok(Box::new(FailingEncoder {
        out_params: out,
        sent: 0,
        fail_send_after: Some(FAIL_ENC_AFTER),
        fail_flush: false,
        queue: Default::default(),
    }))
}

fn make_fail_flush_encoder(params: &CodecParameters) -> Result<Box<dyn Encoder>> {
    let mut out = params.clone();
    out.codec_id = CodecId::new(FAIL_FLUSH_ENC_ID);
    Ok(Box::new(FailingEncoder {
        out_params: out,
        sent: 0,
        fail_send_after: None,
        fail_flush: true,
        queue: Default::default(),
    }))
}

/// Configurable failing sink: which hook errors, and after how many
/// successful payload writes.
#[derive(Clone, Copy, PartialEq)]
enum SinkFault {
    None,
    Start,
    WriteAfter(u64),
    Finish,
}

struct FaultySink {
    fault: SinkFault,
    written: Arc<AtomicUsize>,
}

impl FaultySink {
    fn boxed(fault: SinkFault) -> (Box<dyn JobSink + Send>, Arc<AtomicUsize>) {
        let written = Arc::new(AtomicUsize::new(0));
        (
            Box::new(Self {
                fault,
                written: written.clone(),
            }),
            written,
        )
    }
    fn write(&mut self) -> Result<()> {
        let n = self.written.fetch_add(1, Ordering::SeqCst) as u64 + 1;
        if let SinkFault::WriteAfter(ok) = self.fault {
            if n > ok {
                return Err(Error::other(SINK_WRITE_ERR));
            }
        }
        Ok(())
    }
}

impl JobSink for FaultySink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        if self.fault == SinkFault::Start {
            return Err(Error::other(SINK_START_ERR));
        }
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        self.write()
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        self.write()
    }
    fn finish(&mut self) -> Result<()> {
        if self.fault == SinkFault::Finish {
            return Err(Error::other(SINK_FINISH_ERR));
        }
        Ok(())
    }
}

/// Frame source that dies after 3 frames (`attrdie://` scheme).
struct DyingFrameSource {
    params: CodecParameters,
    emitted: u64,
}

impl FrameSource for DyingFrameSource {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if self.emitted >= 3 {
            return Err(Error::other(SOURCE_ERR));
        }
        let pts = (self.emitted * 16) as i64;
        self.emitted += 1;
        Ok(Frame::Audio(AudioFrame {
            samples: 16,
            pts: Some(pts),
            data: vec![vec![0u8; 32]],
        }))
    }
}

// ───────────────────────── shared setup ─────────────────────────

fn attr_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    // Failing encoders.
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(FAIL_ENC_ID))
            .capabilities(CodecCapabilities::audio(FAIL_ENC_ID).with_encode())
            .encoder(make_fail_send_encoder as EncoderFactory),
    );
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(FAIL_FLUSH_ENC_ID))
            .capabilities(CodecCapabilities::audio(FAIL_FLUSH_ENC_ID).with_encode())
            .encoder(make_fail_flush_encoder as EncoderFactory),
    );
    // Failing filter — pass-through audio, errors on the 4th push.
    ctx.filters.register(
        "attr_failnth",
        Box::new(|_params, inputs| {
            let port = inputs
                .first()
                .cloned()
                .unwrap_or_else(|| PortSpec::audio("in", 8_000, 1, SampleFormat::S16));
            let mut out = port.clone();
            out.name = "out".to_string();
            Ok(Box::new(FailNthFilter {
                inp: vec![port],
                outp: vec![out],
                pushes: 0,
                fail_on: 4,
            }))
        }),
    );
    // Frame source that dies after 3 frames.
    ctx.sources.register_frames("attrdie", |_uri| {
        let mut p = CodecParameters::audio(CodecId::new("attr_pcm"));
        p.sample_rate = Some(8_000);
        p.channels = Some(1);
        p.sample_format = Some(SampleFormat::S16);
        Ok(Box::new(DyingFrameSource {
            params: p,
            emitted: 0,
        }))
    });
    ctx
}

fn json_path(p: &std::path::Path) -> String {
    p.display().to_string().replace('\\', "\\\\")
}

/// Run a single-output job with the given sink override and thread
/// budget; the run must fail; return the `RunFailure`.
fn run_expect_failure(
    job_json: &str,
    output: &str,
    sink: Box<dyn JobSink + Send>,
    threads: usize,
) -> RunFailure {
    let ctx = attr_ctx();
    let job = Job::from_json(job_json).expect("parse job");
    Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override(output, sink)
        .run_reporting()
        .expect_err("run must fail")
}

fn assert_failure(
    f: &RunFailure,
    output: Option<&str>,
    stage: FailureStage,
    track: Option<u32>,
    msg_contains: &str,
) {
    assert_eq!(f.output.as_deref(), output, "output attribution: {f}");
    assert_eq!(f.stage, stage, "stage attribution: {f}");
    assert_eq!(f.track, track, "track attribution: {f}");
    let msg = f.error.to_string();
    assert!(
        msg.contains(msg_contains),
        "root cause lost: got {msg:?}, wanted substring {msg_contains:?}"
    );
}

// ───────────────────────── job-level / prepare ─────────────────────────

#[test]
fn validation_failure_is_job_level_prepare() {
    let ctx = attr_ctx();
    let job = Job::from_json(r#"{"@display": {"audio": [{"from": ""}]}}"#).expect("parse job");
    let f = Executor::new(&job, &ctx)
        .run_reporting()
        .expect_err("must fail validation");
    assert_eq!(f.output, None, "job-level failure has no output: {f}");
    assert_eq!(f.stage, FailureStage::Prepare);
    assert_eq!(f.track, None);
}

#[test]
fn unknown_codec_is_prepare_with_output_key() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_unkcodec_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}", "codec": "attr_no_such_codec"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(&job_json, "@display", sink, threads);
        assert_eq!(
            f.output.as_deref(),
            Some("@display"),
            "threads={threads}: {f}"
        );
        assert_eq!(f.stage, FailureStage::Prepare, "threads={threads}: {f}");
    }
}

// ───────────────────────── per-stage attribution ─────────────────────────

#[test]
fn source_failure_attributes_to_source_stage() {
    for threads in [1usize, 2] {
        let (sink, _) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(
            r#"{"@display": {"audio": [{"from": "attrdie://gen"}]}}"#,
            "@display",
            sink,
            threads,
        );
        assert_failure(&f, Some("@display"), FailureStage::Source, None, SOURCE_ERR);
    }
}

#[test]
fn filter_failure_attributes_to_filter_stage_with_track() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_filter_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{
                "filter": "attr_failnth",
                "input": {{"from": "{}"}}
            }}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(&job_json, "@display", sink, threads);
        assert_failure(
            &f,
            Some("@display"),
            FailureStage::Filter,
            Some(0),
            FILTER_ERR,
        );
    }
}

#[test]
fn encoder_send_failure_attributes_to_encode_stage_with_track() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_encsend_{threads}"));
        let job_json = format!(
            r#"{{"@out": {{"audio": [{{"from": "{}", "codec": "{FAIL_ENC_ID}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(&job_json, "@out", sink, threads);
        assert_failure(
            &f,
            Some("@out"),
            FailureStage::Encode,
            Some(0),
            ENC_SEND_ERR,
        );
    }
}

#[test]
fn encoder_flush_failure_attributes_to_encode_stage_with_track() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_encflush_{threads}"));
        let job_json = format!(
            r#"{{"@out": {{"audio": [{{"from": "{}", "codec": "{FAIL_FLUSH_ENC_ID}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, written) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(&job_json, "@out", sink, threads);
        assert_failure(
            &f,
            Some("@out"),
            FailureStage::Encode,
            Some(0),
            ENC_FLUSH_ERR,
        );
        // The flush failure fires at EOF — the full healthy stream was
        // already encoded + delivered before the trailer-side error.
        assert!(
            written.load(Ordering::SeqCst) > 0,
            "flush-time failure must not retroactively drop the stream"
        );
    }
}

#[test]
fn sink_start_failure_attributes_to_sink_stage() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_sinkstart_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::Start);
        let f = run_expect_failure(&job_json, "@display", sink, threads);
        assert_failure(
            &f,
            Some("@display"),
            FailureStage::Sink,
            None,
            SINK_START_ERR,
        );
    }
}

#[test]
fn sink_write_failure_attributes_to_sink_stage_with_track() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_sinkwrite_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::WriteAfter(3));
        let f = run_expect_failure(&job_json, "@display", sink, threads);
        assert_failure(
            &f,
            Some("@display"),
            FailureStage::Sink,
            Some(0),
            SINK_WRITE_ERR,
        );
    }
}

#[test]
fn sink_finish_failure_attributes_to_sink_finish_stage() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_sinkfinish_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, written) = FaultySink::boxed(SinkFault::Finish);
        let f = run_expect_failure(&job_json, "@display", sink, threads);
        assert_failure(
            &f,
            Some("@display"),
            FailureStage::SinkFinish,
            None,
            SINK_FINISH_ERR,
        );
        // Finish-time failure means the whole stream already landed.
        assert!(written.load(Ordering::SeqCst) > 0);
    }
}

// ───────────────────────── multi-output ─────────────────────────

#[test]
fn multi_output_failure_names_the_failing_output() {
    let src_a = common::stub::touch("attr_multi_a");
    let src_b = common::stub::touch("attr_multi_b");
    let job_json = format!(
        r#"{{
            "outa": {{"audio": [{{"from": "{}"}}]}},
            "outb": {{"audio": [{{"from": "{}"}}]}}
        }}"#,
        json_path(&src_a),
        json_path(&src_b),
    );
    let ctx = attr_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let (sink_a, _) = FaultySink::boxed(SinkFault::None);
    let (sink_b, _) = FaultySink::boxed(SinkFault::WriteAfter(2));
    let f = Executor::new(&job, &ctx)
        .with_threads(4)
        .with_sink_override("outa", sink_a)
        .with_sink_override("outb", sink_b)
        .run_reporting()
        .expect_err("outb must fail");
    assert_failure(
        &f,
        Some("outb"),
        FailureStage::Sink,
        Some(0),
        SINK_WRITE_ERR,
    );
}

#[test]
fn multi_output_both_failing_names_earliest_in_document_order() {
    let src_a = common::stub::touch("attr_multi2_a");
    let src_b = common::stub::touch("attr_multi2_b");
    let job_json = format!(
        r#"{{
            "outa": {{"audio": [{{"from": "{}"}}]}},
            "outb": {{"audio": [{{"from": "{}"}}]}}
        }}"#,
        json_path(&src_a),
        json_path(&src_b),
    );
    let ctx = attr_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let (sink_a, _) = FaultySink::boxed(SinkFault::WriteAfter(5));
    let (sink_b, _) = FaultySink::boxed(SinkFault::WriteAfter(2));
    let f = Executor::new(&job, &ctx)
        .with_threads(4)
        .with_sink_override("outa", sink_a)
        .with_sink_override("outb", sink_b)
        .run_reporting()
        .expect_err("both outputs fail");
    // Document order wins even though outb (later in the document)
    // fails after fewer payloads and therefore earlier in wall time.
    assert_eq!(f.output.as_deref(), Some("outa"), "{f}");
    assert_eq!(f.stage, FailureStage::Sink);
}

// ───────────────────────── parity + spawn path ─────────────────────────

#[test]
fn run_and_run_reporting_observe_the_same_root_cause() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_parity_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let ctx = attr_ctx();
        let job = Job::from_json(&job_json).expect("parse job");
        let (sink, _) = FaultySink::boxed(SinkFault::WriteAfter(3));
        let plain = Executor::new(&job, &ctx)
            .with_threads(threads)
            .with_sink_override("@display", sink)
            .run()
            .expect_err("must fail");
        let (sink2, _) = FaultySink::boxed(SinkFault::WriteAfter(3));
        let reported = Executor::new(&job, &ctx)
            .with_threads(threads)
            .with_sink_override("@display", sink2)
            .run_reporting()
            .expect_err("must fail");
        assert_eq!(plain.to_string(), reported.error.to_string());
        // The From conversion recovers the plain surface exactly.
        let recovered: Error = reported.into();
        assert_eq!(recovered.to_string(), plain.to_string());
    }
}

#[test]
fn stop_reporting_attributes_spawn_path_failures() {
    let src = common::stub::touch("attr_spawn_sinkfail");
    let job_json = format!(
        r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
        json_path(&src)
    );
    let ctx = attr_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let (sink, _) = FaultySink::boxed(SinkFault::WriteAfter(2));
    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", sink)
        .spawn()
        .expect("spawn");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !handle.has_finished() {
        assert!(
            Instant::now() < deadline,
            "executor did not observe the sink failure in time"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let f = handle
        .stop_reporting()
        .expect_err("must surface the failure");
    assert_failure(
        &f,
        Some("@display"),
        FailureStage::Sink,
        Some(0),
        SINK_WRITE_ERR,
    );
}

/// Error KIND (not just the message) survives attribution: a
/// `ResourceExhausted` from a sink (the DoS-cap contract) is still
/// matchable on `RunFailure::error` and on the recovered plain error.
#[test]
fn error_kind_survives_attribution() {
    struct ExhaustedSink;
    impl JobSink for ExhaustedSink {
        fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
            Ok(())
        }
        fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
            Err(Error::resource_exhausted("sink budget"))
        }
        fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
            Err(Error::resource_exhausted("sink budget"))
        }
        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_kind_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let ctx = attr_ctx();
        let job = Job::from_json(&job_json).expect("parse job");
        let f = Executor::new(&job, &ctx)
            .with_threads(threads)
            .with_sink_override("@display", Box::new(ExhaustedSink))
            .run_reporting()
            .expect_err("must fail");
        assert!(f.error.is_resource_exhausted(), "kind lost: {f}");
        let recovered: Error = f.into();
        assert!(recovered.is_resource_exhausted());
    }
}

/// `RunFailure::stats` carries the failing output's partial counters:
/// a mid-stream sink failure reports exactly the frames delivered
/// before the error (deterministic on both paths — the serial pump
/// stops at the failing write, the pipelined mux loop records the
/// error on the first failing write); an EOF-flush encoder failure
/// reports the full healthy stream; a preparation failure reports
/// all-zero counters.
#[test]
fn partial_stats_attached_to_failures() {
    // Mid-stream sink failure: 3 healthy frames landed, the 4th write
    // failed — the snapshot must say 3.
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_stats_sink_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::WriteAfter(3));
        let f = run_expect_failure(&job_json, "@display", sink, threads);
        assert_eq!(
            f.stats.frames_written, 3,
            "threads={threads}: partial frames_written"
        );
        assert!(
            f.stats.packets_read > 0,
            "threads={threads}: packets_read must reflect pre-failure demux work"
        );
    }

    // EOF-flush encoder failure: the whole stream encoded before the
    // flush erred.
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_stats_flush_{threads}"));
        let job_json = format!(
            r#"{{"@out": {{"audio": [{{"from": "{}", "codec": "{FAIL_FLUSH_ENC_ID}"}}]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(&job_json, "@out", sink, threads);
        assert!(
            f.stats.packets_encoded > 0,
            "threads={threads}: flush-time failure must report the encoded stream"
        );
    }

    // Preparation failure: nothing flowed, counters all zero.
    let src = common::stub::touch("attr_stats_prep");
    let job_json = format!(
        r#"{{"@display": {{"audio": [{{"from": "{}", "codec": "attr_no_such_codec"}}]}}}}"#,
        json_path(&src)
    );
    let (sink, _) = FaultySink::boxed(SinkFault::None);
    let f = run_expect_failure(&job_json, "@display", sink, 1);
    assert_eq!(f.stage, FailureStage::Prepare);
    assert_eq!(f.stats.packets_read, 0);
    assert_eq!(f.stats.frames_written, 0);
    assert_eq!(f.stats.packets_encoded, 0);
}

/// Track attribution beyond the trivial index 0: a two-track output
/// whose SECOND track carries the failing encoder attributes
/// `Encode` with `track: Some(1)` on both paths (track 0 is a healthy
/// copy route and keeps flowing until teardown).
#[test]
fn second_track_failure_attributes_track_one() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("attr_track1_{threads}"));
        let job_json = format!(
            r#"{{"@out": {{"audio": [
                {{"from": "{0}"}},
                {{"from": "{0}", "codec": "{FAIL_ENC_ID}"}}
            ]}}}}"#,
            json_path(&src)
        );
        let (sink, _) = FaultySink::boxed(SinkFault::None);
        let f = run_expect_failure(&job_json, "@out", sink, threads);
        assert_failure(
            &f,
            Some("@out"),
            FailureStage::Encode,
            Some(1),
            ENC_SEND_ERR,
        );
    }
}

#[test]
fn independent_track_sink_failure_attributes_to_sink_stage_with_track() {
    const TRACK_SINK_ERR: &str = "independent TrackSink deliberately failed";

    struct FailingTrackSink;

    impl TrackSink for FailingTrackSink {
        fn write_packet(
            &mut self,
            _stream_index: u32,
            _kind: MediaType,
            _packet: Packet,
        ) -> Result<()> {
            Err(Error::other("unexpected packet callback"))
        }

        fn write_frame_lease(
            &mut self,
            _stream_index: u32,
            _kind: MediaType,
            _frame: FrameLease,
        ) -> Result<()> {
            Err(Error::other(TRACK_SINK_ERR))
        }
    }

    struct IndependentFaultSink;

    impl JobSink for IndependentFaultSink {
        fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
            Ok(())
        }

        fn open_track_sinks(
            &mut self,
            tracks: &[TrackSinkInfo],
            _cancellation: CancellationToken,
        ) -> Result<Option<Vec<Box<dyn TrackSink + Send>>>> {
            assert_eq!(tracks.len(), 1);
            Ok(Some(vec![Box::new(FailingTrackSink)]))
        }

        fn write_packet(&mut self, _kind: MediaType, _packet: &Packet) -> Result<()> {
            Err(Error::other("aggregate packet callback must not run"))
        }

        fn write_frame(&mut self, _kind: MediaType, _frame: &Frame) -> Result<()> {
            Err(Error::other("aggregate frame callback must not run"))
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    let src = common::stub::touch("attr_independent_track_sink");
    let job_json = format!(
        r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
        json_path(&src)
    );
    let ctx = attr_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", Box::new(IndependentFaultSink))
        .spawn()
        .expect("spawn");

    let deadline = Instant::now() + Duration::from_secs(20);
    while !handle.has_finished() {
        assert!(
            Instant::now() < deadline,
            "executor did not observe independent TrackSink failure in time"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let failure = handle
        .stop_reporting()
        .expect_err("independent TrackSink failure must surface");
    assert_failure(
        &failure,
        Some("@display"),
        FailureStage::Sink,
        Some(0),
        TRACK_SINK_ERR,
    );
}

/// The mux loop's `sink.barrier` record site: a sink that errors on
/// the seek barrier attributes `Sink` with the barrier's track index,
/// through the spawn-path reporting surface.
#[test]
fn barrier_failure_attributes_to_sink_stage() {
    struct BarrierFaultSink;
    impl JobSink for BarrierFaultSink {
        fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
            Ok(())
        }
        fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
            Ok(())
        }
        fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
        fn barrier(&mut self, _kind: oxideav_pipeline::BarrierKind) -> Result<()> {
            Err(Error::other("barrier sink deliberately failed"))
        }
    }
    let src = common::stub::touch("attr_barrier_fault");
    let job_json = format!(
        r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
        json_path(&src)
    );
    let ctx = attr_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", Box::new(BarrierFaultSink))
        .spawn()
        .expect("spawn");
    handle
        .seek(0, 8_000, oxideav_core::TimeBase::new(1, 8_000))
        .expect("dispatch seek");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !handle.has_finished() {
        assert!(
            Instant::now() < deadline,
            "executor did not observe the barrier failure in time"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let f = handle
        .stop_reporting()
        .expect_err("barrier failure must surface");
    assert_failure(
        &f,
        Some("@display"),
        FailureStage::Sink,
        Some(0),
        "barrier sink deliberately failed",
    );
}
