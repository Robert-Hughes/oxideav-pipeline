//! Pipelined (stage-per-thread) executor.
//!
//! Called by [`Executor::run`] when the thread budget is `≥ 2`. Spawns
//! one worker thread per pipeline stage per track, connected by bounded
//! `mpsc::sync_channel`s, and drives the mux/sink loop on the caller's
//! thread. Sinks therefore don't need to be `Send`.
//!
//! Data flow per output:
//!
//! ```text
//!   [one source thread per URI]
//!     bytes / packet shape ──► per-track packet channel ─┐
//!                                                         ├─► decode ─► filter… ─► encode ─► output channel
//!                                                         ┴─► (copy mode: output channel directly)
//!     frame shape ──────────► per-track frame channel ──► filter… ─► encode-or-fanout ─► output channel
//!
//!   main thread (mux loop): recv across all output channels → sink.write_packet / write_frame
//! ```
//!
//! Bytes-shape sources run a demuxer thread; packet-shape sources
//! (RTMP, …) run the same fan-out without the container layer;
//! frame-shape sources (generators, rendered scenes) feed the
//! per-track frame chains directly — no demux, no decode. Seeks
//! against packet- / frame-shape sources are answered with
//! [`BarrierKind::SeekRejected`] (no seek surface on those traits).
//!
//! End-of-stream is signalled with [`Msg::Eof`] rather than by dropping
//! the sender, so downstream stages can reliably flush their internal
//! buffers before exiting. Errors in any stage are funnelled through
//! [`AbortState`]; the first error wins, other stages bail cleanly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use oxideav_core::Demuxer;
use oxideav_core::{
    CancellationToken, CodecParameters, Error, Frame, FrameLease, FrameSource, MediaType, Packet,
    PacketSource, Result, StreamInfo, TimeBase,
};
use oxideav_core::{Decoder, Encoder};

use crate::executor::{
    flush_frame_stage_emit, frame_stage_failure_kind, run_frame_stage_emit, ExecutorStats,
    FrameStage, JobSink, SourcePump, TrackRuntime, TrackSink, TrackSinkInfo,
};
use crate::failure::{attribute, FailureStage, StageFailure, StageResult};

/// Flow-barrier kind in [`Msg::Barrier`]. Broadcast by the demuxer
/// stage when it receives a [`SeekCmd`] from the
/// [`crate::ExecutorHandle`]. There are two outcomes per command:
///
/// * `SeekFlush` — the demuxer's `seek_to` returned `Ok`. Workers
///   drop in-flight state; the engine re-anchors its clock.
/// * `SeekRejected` — the demuxer's `seek_to` returned `Err`
///   (typically `Error::Unsupported`, e.g. an MP3 stream without a
///   Xing TOC or any container that hasn't implemented seek_to).
///   The demuxer keeps playing from its current position so the
///   pipeline stays alive; the engine should disable its seek UI
///   for the rest of the session.
///
/// Each successful and each rejected seek consumes ONE generation
/// value, incremented in lock-step with the demuxer's internal
/// counter. Adding new kinds is non-breaking: every worker treats
/// unknown kinds as "forward unchanged".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierKind {
    /// Seek-induced flush. Workers reset codec / filter state, then
    /// forward this barrier downstream so the sink can drop any in-
    /// flight frames buffered above it.
    ///
    /// `generation` is incremented by the demuxer on every seek so the
    /// engine can correlate `seek()` calls with their corresponding
    /// barrier emission and ignore pre-seek payload still in flight.
    ///
    /// `landed_pts` is the actual position the demuxer reached
    /// (typically the largest keyframe ≤ requested target), expressed
    /// in `time_base` units. Engines re-anchor their master clock at
    /// this exact value rather than guessing from the next packet's
    /// pts — video lands on a keyframe (≤ target) while audio lands
    /// on the next packet (≥ target), so any "guess from the next
    /// audio frame" heuristic is typically off by 50-200 ms.
    SeekFlush {
        generation: u32,
        landed_pts: i64,
        time_base: TimeBase,
    },
    /// Demuxer rejected the corresponding [`SeekCmd`] — `seek_to`
    /// returned an error. This barrier carries the same `generation`
    /// the matching `SeekFlush` would have used, so engines that
    /// track seek-in-flight by generation can clear that bookkeeping
    /// uniformly. Workers should NOT reset their codec state on
    /// `SeekRejected` because the demuxer kept reading from its
    /// previous position; the in-flight frames are still valid.
    /// Engines should disable seek UI for the session.
    SeekRejected { generation: u32 },
}

/// Command sent to the demuxer stage by [`crate::ExecutorHandle::seek`].
/// Carries the `generation` value that the demuxer will stamp on the
/// resulting `SeekFlush` / `SeekRejected` barrier — assigned by the
/// handle's atomic counter at `seek()` time so the caller can correlate
/// its dispatch with the eventual barrier (returned from
/// [`crate::ExecutorHandle::seek_with_generation`]).
///
/// The demuxer no longer keeps its own private counter — every barrier
/// it emits in response to a `SeekCmd` carries this exact value
/// verbatim. This guarantees `handle.seek_with_generation(...)?` and
/// `BarrierKind::Seek* { generation, .. }` are in lockstep even under
/// rapid bursts of seeks (e.g. the user holding `→` for a half-second
/// scrubbing across a video — pre-fix the handle had to mirror the
/// demuxer's counter and any dropped / out-of-order delivery would
/// silently desync the engine's seek-pending bookkeeping).
#[derive(Clone, Copy, Debug)]
pub struct SeekCmd {
    pub stream_idx: u32,
    pub pts: i64,
    pub time_base: TimeBase,
    /// Caller-assigned generation; the demuxer copies this value into
    /// every resulting barrier so the caller can match its dispatch
    /// with the corresponding `SeekFlush` / `SeekRejected`.
    pub generation: u32,
}

/// Per-frame progress event consumed by [`crate::ExecutorHandle::try_progress`].
/// Updated by the mux loop on every `Msg::Data` carrying frame/packet pts;
/// the engine polls this once per tick for the status bar.
///
/// `queue_bytes` reports the current in-flight packet-byte total tracked by
/// [`crate::Executor::with_max_queue_bytes`]'s shared accountant. This gives
/// the engine a diagnostic surface for back-pressure: a value that pins to
/// the configured ceiling indicates the demuxer is parking on the byte
/// budget waiting for the consumer to drain, whereas a value that hovers
/// near zero means the byte ceiling isn't binding (the count caps or
/// downstream blocks first). When `with_max_queue_bytes(0)` (the default,
/// no byte ceiling) is in effect, this field is always `0` — the budget
/// short-circuits its accounting and the demuxer never parks.
///
/// `elapsed_micros` reports the wall-clock microseconds since the pipelined
/// runner started — specifically since the `Instant` taken just before the
/// first worker thread spawned, which is also when the demuxer begins
/// reading. The mux loop stamps this value on every `Progress` it emits
/// (mid-run and at EOF) so engines can derive headline diagnostics without
/// keeping their own wall-clock parallel to `pts`:
///
/// * **Realtime ratio.** For a single time_base, an engine can compute
///   `pts_micros / elapsed_micros` and read off transcode speed (`> 1.0`
///   = faster than realtime; `< 1.0` = slower). The serial path doesn't
///   emit progress at all so this is only meaningful on the pipelined
///   runner — which is the only one anyone watches anyway.
/// * **Realtime drift on live sources.** For a real-time source (RTMP,
///   capture card), the engine compares the latest `pts` to
///   `elapsed_micros` and detects when the pipeline is falling behind
///   the source clock before audio-ring drain surfaces it.
/// * **EOF wall-clock total.** The `eof: true` progress event reports
///   the total wall-clock the run took, so a CLI tool doesn't need to
///   wrap `executor.run()` in its own `Instant::now()` bracket just to
///   report "encoded N frames in 4.21 s".
///
/// The serial path doesn't emit `Progress` (no progress channel is wired
/// in `Executor::run`), so this field is only ever non-zero on the
/// pipelined runner reached via `Executor::spawn` + `ExecutorHandle`.
///
/// **Pattern-match consumers**: new fields will land here as the engine
/// surface grows. Use the struct-update syntax (`Progress { pts, .. }`)
/// or read fields by name to stay forward-compatible.
#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
    pub pts: Option<i64>,
    pub frames: u64,
    pub eof: bool,
    /// Current in-flight packet-byte total (sum of `Packet::data.len()`
    /// for every packet that has left the demuxer but not yet been
    /// consumed by the next stage). Always `0` when no byte ceiling is
    /// configured via [`crate::Executor::with_max_queue_bytes`].
    pub queue_bytes: u64,
    /// Wall-clock microseconds since the pipelined runner's baseline
    /// `Instant` — captured just before the first worker thread spawns
    /// (which is also when the demuxer begins reading). Always
    /// monotonically non-decreasing across consecutive emissions from
    /// the same handle. `Default::default()` is `0`, which matches the
    /// "no progress wired" path used by `Executor::run` (the serial
    /// runner doesn't emit `Progress` at all).
    pub elapsed_micros: u64,
    /// Cumulative count of packets the decoder skipped because of a
    /// recoverable per-packet error — either `send_packet` returned an
    /// error (the packet never produced a frame) or the subsequent
    /// `receive_frame` errored before yielding any output. Each event
    /// is logged via the existing `eprintln!` path; this counter lets an
    /// engine surface the same information in its status bar without
    /// scraping stderr, and lets a stress harness assert on the
    /// tolerance contract pinned by `tests/decoder_error_tolerance.rs`.
    ///
    /// Monotonically non-decreasing across consecutive emissions from
    /// the same handle. Always `0` on copy-only outputs (no decoder is
    /// instantiated) and on a clean stream (no skips occurred). The
    /// serial path (`Executor::run`) doesn't emit `Progress` at all,
    /// so this field is only ever non-zero on the pipelined runner
    /// reached via `Executor::spawn`.
    pub packets_skipped: u64,
    /// Cumulative count of packets the demuxer has read from the source.
    /// This mirrors [`crate::executor::ExecutorStats::packets_read`] but
    /// is sampled live on every `Progress` emission so an engine can
    /// detect a stalled decoder (the demuxer keeps reading — `packets_read`
    /// climbs — but `frames` and `packets_skipped` stay flat, so the
    /// decode stage isn't draining its inbox).
    ///
    /// Headroom = `packets_read - frames - packets_skipped` is the count
    /// of demuxed packets the decode stage hasn't yet resolved (still in
    /// the queue, or still pending inside the decoder waiting for more
    /// input). A value pinned at the channel-depth budget combined with
    /// a flat `frames` field is the diagnostic signature of a wedged
    /// decoder.
    ///
    /// Monotonically non-decreasing across consecutive emissions from
    /// the same handle. The serial path (`Executor::run`) doesn't emit
    /// `Progress` at all, so this field is only ever non-zero on the
    /// pipelined runner reached via `Executor::spawn`. At EOF the
    /// `packets_read` value matches the final
    /// [`crate::executor::ExecutorStats::packets_read`] snapshot.
    pub packets_read: u64,
    /// Cumulative count of packets the encoder has produced. Mirrors
    /// [`crate::executor::ExecutorStats::packets_encoded`] but sampled
    /// live on every `Progress` emission so an engine can detect a
    /// stalled encoder without waiting for EOF: the decoder is making
    /// progress (`frames` and/or upstream stages keep ticking) but the
    /// encoder isn't emitting packets (`packets_encoded` stays flat).
    /// Pre-r209 `packets_encoded` was only readable on the final
    /// `ExecutorStats` snapshot, so a stress harness had to wait for
    /// the run to finish before it could even tell whether the encode
    /// stage had been running at all — and a CLI status bar couldn't
    /// surface "encoded N packets / decoded M frames" without
    /// instrumenting the encoder externally.
    ///
    /// Monotonically non-decreasing across consecutive emissions from
    /// the same handle. Always `0` on copy-only outputs (no encoder is
    /// instantiated — the staged runner skips `run_encode_stage` and
    /// the counter is never bumped). The serial path (`Executor::run`)
    /// doesn't emit `Progress` at all, so this field is only ever
    /// non-zero on the pipelined runner reached via `Executor::spawn`.
    /// At EOF the `packets_encoded` value matches the final
    /// [`crate::executor::ExecutorStats::packets_encoded`] snapshot.
    pub packets_encoded: u64,
    /// Cumulative count of packets the copy stage forwarded into the mux
    /// loop. Mirrors [`crate::executor::ExecutorStats::packets_copied`]
    /// but sampled live on every `Progress` emission so an engine can
    /// distinguish the copy and transcode sides of a mixed output without
    /// waiting for EOF — e.g. a remux job whose audio track copies while
    /// the video track transcodes will see `packets_copied` and
    /// `packets_encoded` advance independently, and a wedged copy stage
    /// shows up as a flat `packets_copied` while `packets_read` keeps
    /// climbing (the demuxer is still serving packets but they're not
    /// reaching the mux loop).
    ///
    /// Monotonically non-decreasing across consecutive emissions from
    /// the same handle. Always `0` on transcode-only outputs (every
    /// track instantiates a decoder + encoder and no track uses the
    /// copy path) and on outputs whose every track is rejected. The
    /// serial path (`Executor::run`) doesn't emit `Progress` at all,
    /// so this field is only ever non-zero on the pipelined runner
    /// reached via `Executor::spawn`. At EOF the `packets_copied` value
    /// matches the final
    /// [`crate::executor::ExecutorStats::packets_copied`] snapshot.
    pub packets_copied: u64,
}

/// Packet-channel depth. Small enough that a stalled consumer back-pressures
/// the demuxer before memory blows up; large enough to amortise the mutex
/// cost on each send.
const PACKET_CAP: usize = 16;

/// Frame-channel depth. Smaller than `PACKET_CAP` because decoded frames
/// are much larger than compressed packets.
const FRAME_CAP: usize = 8;

/// Per-track channel-depth budget for the pipelined staged executor.
///
/// Each track in a pipelined run is plumbed by two bounded
/// `mpsc::sync_channel`s:
/// * a **packet** channel between the demuxer and the per-track copy /
///   decode worker, and one between every output worker and the mux
///   loop (sized at `packets`);
/// * a **frame** channel between every pair of frame stages
///   (decode → filter / pix-convert → encode), sized at `frames`.
///
/// The defaults — 16 packets and 8 frames — back-pressure a stalled
/// consumer before memory blows up while still amortising the channel's
/// mutex cost on each send. Operators with tight memory budgets (e.g.
/// embedded playback) can shrink the depth via
/// [`Executor::with_channel_caps`](crate::Executor::with_channel_caps);
/// high-throughput offline transcodes can raise it to let bursty
/// decoders coast on the queue depth instead of blocking on the
/// downstream encoder.
///
/// **Memory upper bound (per output, rough):**
/// ```text
///     N_tracks * (packets * packet_size + frames * frame_size)
/// ```
/// — every track holds at most `packets` packets in its demuxer→worker
/// queue and at most `frames` frames in its inter-stage queues.
///
/// Both fields must be ≥ 1. Zero is silently promoted to one (the
/// underlying `sync_channel` rejects a depth of zero — that's a
/// rendezvous channel and would serialise the entire pipeline).
#[derive(Clone, Copy, Debug)]
pub struct ChannelCaps {
    /// Depth of the per-track packet channels (demuxer → worker, and
    /// worker → mux loop). Default: 16.
    pub packets: usize,
    /// Depth of the per-stage frame channels (decode → filter →
    /// pix-convert → encode). Default: 8.
    pub frames: usize,
}

impl Default for ChannelCaps {
    fn default() -> Self {
        Self {
            packets: PACKET_CAP,
            frames: FRAME_CAP,
        }
    }
}

impl ChannelCaps {
    /// Sanitised values that the staged runner actually uses. Both
    /// fields are clamped to a minimum of 1 so a caller passing `0`
    /// gets the smallest non-rendezvous queue rather than a panic
    /// from `sync_channel(0)` being a rendezvous channel.
    pub(crate) fn resolved(&self) -> (usize, usize) {
        (self.packets.max(1), self.frames.max(1))
    }
}

/// Memory-bounded back-pressure on the demuxer→worker packet queues.
///
/// [`ChannelCaps`] bounds the queues by *element count* — at most
/// `packets` packets per track sit in the demuxer→worker channel. That
/// is the right knob when packet sizes are uniform, but a single
/// pathological packet (a tracker module that delivers the whole song
/// in one packet, an intra-only keyframe of a 4K stream, a JPEG-2000
/// codestream) can be megabytes on its own. Sixteen of those is a
/// quarter-gigabyte resident before the count cap even notices.
///
/// `QueueBudget` adds an orthogonal *byte* ceiling. The demuxer adds
/// each packet's `data.len()` to a shared atomic before fanning it out
/// to its routes, and the consuming stage (copy or decode) subtracts
/// the same count the instant it receives the packet. Before reading
/// the next packet the demuxer parks while the running total is at or
/// above `max`, so the bytes physically buffered in the packet channels
/// never run far past the ceiling (one in-flight packet may straddle
/// it — we admit the packet that crosses the line rather than deadlock
/// on a lone packet larger than the whole budget).
///
/// `max == 0` means "no byte ceiling" — the count caps alone govern,
/// preserving the historical behaviour for callers that never opt in.
pub(crate) struct QueueBudget {
    in_flight: AtomicU64,
    max: u64,
}

impl QueueBudget {
    /// `max` bytes; `0` disables the byte ceiling entirely.
    pub(crate) fn new(max: u64) -> Arc<Self> {
        Arc::new(Self {
            in_flight: AtomicU64::new(0),
            max,
        })
    }

    /// Whether a byte ceiling is in force. When `false`, `admit` /
    /// `release` are cheap no-ops and the demuxer never parks.
    fn enabled(&self) -> bool {
        self.max > 0
    }

    /// Current in-flight packet-byte total. Returned verbatim to the
    /// engine via [`Progress::queue_bytes`] so callers can observe how
    /// close the demuxer is to the byte ceiling. Returns `0` when the
    /// ceiling is disabled (`max == 0`) — the accountant short-circuits
    /// in that mode and the counter never moves off zero.
    pub(crate) fn in_flight(&self) -> u64 {
        if self.enabled() {
            self.in_flight.load(Ordering::SeqCst)
        } else {
            0
        }
    }

    /// Account `n` bytes as entering the packet queues. Called by the
    /// demuxer once per packet, just before it fans the packet out.
    fn admit(&self, n: u64) {
        if self.enabled() {
            self.in_flight.fetch_add(n, Ordering::SeqCst);
        }
    }

    /// Account `n` bytes as leaving the packet queues. Called by the
    /// consuming stage the instant it receives a packet off the channel.
    /// Saturating so a double-release (shouldn't happen) can't wrap.
    fn release(&self, n: u64) {
        if self.enabled() {
            let mut cur = self.in_flight.load(Ordering::SeqCst);
            loop {
                let next = cur.saturating_sub(n);
                match self.in_flight.compare_exchange_weak(
                    cur,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(observed) => cur = observed,
                }
            }
        }
    }

    /// Block the calling (demuxer) thread while the in-flight byte total
    /// is at or above the ceiling. Returns early if `abort` is set so a
    /// stop/quit doesn't strand the demuxer here. A short park (1 ms)
    /// between polls keeps a stalled consumer from spinning a core; the
    /// release path is event-light enough that a condvar would be
    /// over-engineering for the ≤16-element queues this guards.
    fn wait_below_ceiling(&self, abort: &AbortState) {
        if !self.enabled() {
            return;
        }
        while self.in_flight.load(Ordering::SeqCst) >= self.max {
            if abort.is_aborted() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Messages across channels.
///
/// * `Data` — payload (packet/frame).
/// * `StreamUpdate` — authoritative decoded stream metadata discovered after
///   `JobSink::start()`; ordered before frames that use the new format.
/// * `Barrier` — flow-control marker. Today only `SeekFlush` is in use;
///   workers reset codec/filter state and forward unchanged.
/// * `Eof` — in-band end-of-stream so downstream stages can flush state
///   before exiting.
enum Msg<T> {
    Data(T),
    StreamUpdate(Box<StreamInfo>),
    Barrier(BarrierKind),
    Eof,
}

/// Shared counters. Each worker increments its relevant field; the mux
/// thread reads them out at the end into [`ExecutorStats`].
#[derive(Default)]
struct PipelineCounters {
    packets_read: AtomicU64,
    packets_copied: AtomicU64,
    packets_encoded: AtomicU64,
    frames_decoded: AtomicU64,
    frames_written: AtomicU64,
    /// Decoder-skip counter. Bumped by the decode stage on every
    /// packet whose `send_packet` errored, and on every packet whose
    /// downstream `receive_frame` errored before yielding a frame —
    /// matching the two `eprintln!` branches in `run_decode_stage`.
    /// Surfaced to the engine via [`Progress::packets_skipped`] and
    /// to the final stats via [`ExecutorStats::packets_skipped`].
    packets_skipped: AtomicU64,
}

impl PipelineCounters {
    fn snapshot(&self) -> ExecutorStats {
        ExecutorStats {
            packets_read: self.packets_read.load(Ordering::SeqCst),
            packets_copied: self.packets_copied.load(Ordering::SeqCst),
            packets_encoded: self.packets_encoded.load(Ordering::SeqCst),
            frames_decoded: self.frames_decoded.load(Ordering::SeqCst),
            frames_written: self.frames_written.load(Ordering::SeqCst),
            packets_skipped: self.packets_skipped.load(Ordering::SeqCst),
        }
    }
}

/// Shared state used to coordinate clean shutdown across all worker
/// threads in one output's pipeline. Held inside an `Arc` so each
/// worker can poll the flag and so [`crate::ExecutorHandle`] can
/// flip it from the outside.
pub(crate) struct AbortState {
    /// Shared cancellation primitive. Worker loops poll it and blocking
    /// decoder waits register wake targets against the same token.
    cancellation: CancellationToken,
    /// First `Err(_)` seen, with its stage/track attribution. Later
    /// errors are dropped so the caller gets the root cause rather
    /// than a cascading symptom.
    first_err: Mutex<Option<StageFailure>>,
}

impl AbortState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            cancellation: CancellationToken::new(),
            first_err: Mutex::new(None),
        })
    }

    pub(crate) fn is_aborted(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub(crate) fn request_abort(&self) {
        self.cancellation.cancel();
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn record_failure(&self, f: StageFailure) {
        let mut slot = self.first_err.lock().unwrap();
        if slot.is_none() {
            *slot = Some(f);
        }
        drop(slot);
        self.cancellation.cancel();
    }

    fn take_failure(&self) -> Option<StageFailure> {
        self.first_err.lock().unwrap().take()
    }
}

/// One per-track output channel item — retains the track index so the
/// mux thread can tag packets with the right stream index.
struct OutputItem {
    track_index: u32,
    kind: MediaType,
    payload: OutputPayload,
}

enum OutputPayload {
    Packet(Packet),
    Frame(FrameLease),
}

enum DeliveryStatus {
    Delivered,
    Closed,
}

enum TrackTerminalTarget {
    Aggregate(SyncSender<Msg<OutputItem>>),
    Independent(Box<dyn TrackSink + Send>),
}

struct TrackTerminal {
    track_index: u32,
    kind: MediaType,
    target: TrackTerminalTarget,
    abort: Arc<AbortState>,
}

impl TrackTerminal {
    fn aggregate_sender(&self) -> Option<SyncSender<Msg<OutputItem>>> {
        match &self.target {
            TrackTerminalTarget::Aggregate(tx) => Some(tx.clone()),
            TrackTerminalTarget::Independent(_) => None,
        }
    }

    fn is_independent(&self) -> bool {
        matches!(&self.target, TrackTerminalTarget::Independent(_))
    }

    fn record_sink_result(
        abort: &Arc<AbortState>,
        track_index: u32,
        result: Result<()>,
    ) -> Result<DeliveryStatus> {
        match result {
            Ok(()) => Ok(DeliveryStatus::Delivered),
            Err(error) if error.is_cancelled() && abort.is_aborted() => Err(error),
            Err(error) => {
                abort.record_failure(StageFailure::new(
                    FailureStage::Sink,
                    Some(track_index),
                    error,
                ));
                Err(Error::cancelled(
                    "pipeline: independent TrackSink failed; aborting sibling stages",
                ))
            }
        }
    }

    fn write_packet(&mut self, packet: Packet) -> Result<DeliveryStatus> {
        let abort = self.abort.clone();
        let track_index = self.track_index;
        match &mut self.target {
            TrackTerminalTarget::Aggregate(tx) => match tx.send(Msg::Data(OutputItem {
                track_index,
                kind: self.kind,
                payload: OutputPayload::Packet(packet),
            })) {
                Ok(()) => Ok(DeliveryStatus::Delivered),
                Err(_) => Ok(DeliveryStatus::Closed),
            },
            TrackTerminalTarget::Independent(sink) => Self::record_sink_result(
                &abort,
                track_index,
                sink.write_packet(track_index, self.kind, packet),
            ),
        }
    }

    fn write_frame(&mut self, frame: FrameLease) -> Result<DeliveryStatus> {
        let abort = self.abort.clone();
        let track_index = self.track_index;
        match &mut self.target {
            TrackTerminalTarget::Aggregate(tx) => match tx.send(Msg::Data(OutputItem {
                track_index,
                kind: self.kind,
                payload: OutputPayload::Frame(frame),
            })) {
                Ok(()) => Ok(DeliveryStatus::Delivered),
                Err(_) => Ok(DeliveryStatus::Closed),
            },
            TrackTerminalTarget::Independent(sink) => Self::record_sink_result(
                &abort,
                track_index,
                sink.write_frame_lease(track_index, self.kind, frame),
            ),
        }
    }

    fn stream_update(&mut self, stream: StreamInfo) -> Result<DeliveryStatus> {
        let abort = self.abort.clone();
        let track_index = self.track_index;
        match &mut self.target {
            TrackTerminalTarget::Aggregate(tx) => {
                match tx.send(Msg::StreamUpdate(Box::new(stream))) {
                    Ok(()) => Ok(DeliveryStatus::Delivered),
                    Err(_) => Ok(DeliveryStatus::Closed),
                }
            }
            TrackTerminalTarget::Independent(sink) => {
                Self::record_sink_result(&abort, track_index, sink.stream_update(&stream))
            }
        }
    }

    fn barrier(&mut self, barrier: BarrierKind) -> Result<DeliveryStatus> {
        let abort = self.abort.clone();
        let track_index = self.track_index;
        match &mut self.target {
            TrackTerminalTarget::Aggregate(tx) => match tx.send(Msg::Barrier(barrier)) {
                Ok(()) => Ok(DeliveryStatus::Delivered),
                Err(_) => Ok(DeliveryStatus::Closed),
            },
            TrackTerminalTarget::Independent(sink) => {
                Self::record_sink_result(&abort, track_index, sink.barrier(barrier))
            }
        }
    }

    fn eof(&mut self) {
        if let TrackTerminalTarget::Aggregate(tx) = &self.target {
            let _ = tx.send(Msg::Eof);
        }
    }
}

enum FrameDownstream {
    Channel(SyncSender<Msg<FrameLease>>),
    Terminal(TrackTerminal),
}

impl FrameDownstream {
    fn writes_sink_directly(&self) -> bool {
        matches!(self, FrameDownstream::Terminal(terminal) if terminal.is_independent())
    }
    fn send_frame(&mut self, frame: FrameLease) -> Result<DeliveryStatus> {
        match self {
            FrameDownstream::Channel(tx) => match tx.send(Msg::Data(frame)) {
                Ok(()) => Ok(DeliveryStatus::Delivered),
                Err(_) => Ok(DeliveryStatus::Closed),
            },
            FrameDownstream::Terminal(terminal) => terminal.write_frame(frame),
        }
    }

    fn stream_update(&mut self, stream: StreamInfo) -> Result<DeliveryStatus> {
        match self {
            FrameDownstream::Channel(tx) => match tx.send(Msg::StreamUpdate(Box::new(stream))) {
                Ok(()) => Ok(DeliveryStatus::Delivered),
                Err(_) => Ok(DeliveryStatus::Closed),
            },
            FrameDownstream::Terminal(terminal) => terminal.stream_update(stream),
        }
    }

    fn barrier(&mut self, barrier: BarrierKind) -> Result<DeliveryStatus> {
        match self {
            FrameDownstream::Channel(tx) => match tx.send(Msg::Barrier(barrier)) {
                Ok(()) => Ok(DeliveryStatus::Delivered),
                Err(_) => Ok(DeliveryStatus::Closed),
            },
            FrameDownstream::Terminal(terminal) => terminal.barrier(barrier),
        }
    }

    fn eof(&mut self) {
        match self {
            FrameDownstream::Channel(tx) => {
                let _ = tx.send(Msg::Eof);
            }
            FrameDownstream::Terminal(terminal) => terminal.eof(),
        }
    }
}

fn deliver_frame_downstream(
    downstream: &mut FrameDownstream,
    frame: FrameLease,
    abort: &Arc<AbortState>,
    counters: &PipelineCounters,
) -> Result<bool> {
    let direct = downstream.writes_sink_directly();
    match downstream.send_frame(frame) {
        Ok(DeliveryStatus::Delivered) => {
            if direct {
                counters.frames_written.fetch_add(1, Ordering::SeqCst);
            }
            Ok(true)
        }
        Ok(DeliveryStatus::Closed) => {
            abort.request_abort();
            Ok(false)
        }
        Err(e) if e.is_cancelled() && abort.is_aborted() => Ok(false),
        Err(e) => Err(e),
    }
}

fn deliver_stream_update(
    downstream: &mut FrameDownstream,
    stream: StreamInfo,
    abort: &Arc<AbortState>,
) -> Result<bool> {
    match downstream.stream_update(stream) {
        Ok(DeliveryStatus::Delivered) => Ok(true),
        Ok(DeliveryStatus::Closed) => {
            abort.request_abort();
            Ok(false)
        }
        Err(e) if e.is_cancelled() && abort.is_aborted() => Ok(false),
        Err(e) => Err(e),
    }
}

fn deliver_frame_barrier(
    downstream: &mut FrameDownstream,
    barrier: BarrierKind,
    abort: &Arc<AbortState>,
) -> Result<bool> {
    match downstream.barrier(barrier) {
        Ok(DeliveryStatus::Delivered) => Ok(true),
        Ok(DeliveryStatus::Closed) => {
            abort.request_abort();
            Ok(false)
        }
        Err(e) if e.is_cancelled() && abort.is_aborted() => Ok(false),
        Err(e) => Err(e),
    }
}

/// Optional control bundle for [`run_pipelined`]. `seek_rx` is consumed
/// by the (single) demuxer thread that picks it up; `progress_tx` is
/// updated by the mux loop on every data/barrier event.
///
/// Both fields are independent — a caller can wire only one if needed.
/// Used today by [`crate::Executor::spawn`]; the synchronous
/// [`crate::Executor::run`] passes `None` and gets the legacy behaviour.
pub(crate) struct PipelineControl {
    pub seek_rx: Option<Receiver<SeekCmd>>,
    pub progress_tx: Option<SyncSender<Progress>>,
    pub abort: Option<Arc<AbortState>>,
    /// Per-track channel-depth budget. `None` means use the
    /// [`ChannelCaps::default()`] (16 packets, 8 frames). Threaded
    /// through from [`crate::Executor::with_channel_caps`].
    pub caps: Option<ChannelCaps>,
    /// Aggregate byte ceiling on the demuxer→worker packet queues.
    /// `0` (the default) disables the byte ceiling, leaving only the
    /// count caps. Threaded through from
    /// [`crate::Executor::with_max_queue_bytes`].
    pub max_queue_bytes: u64,
    /// Failed-output disposal opt-in, threaded through from
    /// [`crate::Executor::with_discard_failed_outputs`]. On any
    /// failure path the sink receives
    /// [`JobSink::abandon`](crate::JobSink::abandon) before the error
    /// returns; a clean stop (abort without a recorded error) still
    /// finalises via `finish`.
    pub discard_on_failure: bool,
}

/// Run one output's pipeline. The caller has already instantiated all
/// decoders/filters/encoders via `TrackRuntime::instantiate`, opened the
/// sources, and prepared the sink (but not called `start` on it).
pub(crate) fn run_pipelined(
    pipelines: Vec<TrackRuntime>,
    sources_by_uri: HashMap<String, SourcePump>,
    sink: Box<dyn JobSink + Send>,
    out_streams: Vec<StreamInfo>,
    caps: Option<ChannelCaps>,
    max_queue_bytes: u64,
    discard_on_failure: bool,
) -> StageResult<ExecutorStats> {
    run_pipelined_inner(
        pipelines,
        sources_by_uri,
        sink,
        out_streams,
        PipelineControl {
            seek_rx: None,
            progress_tx: None,
            abort: None,
            caps,
            max_queue_bytes,
            discard_on_failure,
        },
    )
}

/// Like [`run_pipelined`] but with explicit control wiring — used by
/// [`crate::Executor::spawn`] to plumb the seek + progress + abort
/// channels through to the source-pump / mux loop.
pub(crate) fn run_pipelined_with_control(
    pipelines: Vec<TrackRuntime>,
    sources_by_uri: HashMap<String, SourcePump>,
    sink: Box<dyn JobSink + Send>,
    out_streams: Vec<StreamInfo>,
    control: PipelineControl,
) -> StageResult<ExecutorStats> {
    run_pipelined_inner(pipelines, sources_by_uri, sink, out_streams, control)
}

pub(crate) fn run_pipelined_inner(
    mut pipelines: Vec<TrackRuntime>,
    sources_by_uri: HashMap<String, SourcePump>,
    mut sink: Box<dyn JobSink + Send>,
    out_streams: Vec<StreamInfo>,
    control: PipelineControl,
) -> StageResult<ExecutorStats> {
    let discard_on_failure = control.discard_on_failure;
    // Failed-output disposal (opt-in): on any failure path the sink
    // gets `abandon()` (drop partial artifacts) instead of `finish()`.
    // Bundled in a closure so every early-return site below stays a
    // one-liner; disposal errors never mask the run error.
    let fail_sink = |sink: &mut Box<dyn JobSink + Send>, failure: StageFailure| -> StageFailure {
        if discard_on_failure {
            let _ = sink.abandon();
        }
        failure
    };
    // External abort takes precedence so callers (e.g. `ExecutorHandle`)
    // can pre-arm cancellation before the workers spawn. The same token is
    // handed to independent TrackSinks so a blocked sink can wake on abort.
    let abort = control.abort.unwrap_or_else(AbortState::new);
    let cancellation = abort.cancellation_token();

    if let Err(e) = sink.start(&out_streams) {
        return Err(fail_sink(&mut sink, attribute(FailureStage::Sink, None)(e)));
    }

    let track_infos: Vec<TrackSinkInfo> = pipelines
        .iter()
        .enumerate()
        .map(|(track_idx, _)| TrackSinkInfo {
            track_index: track_idx as u32,
            stream: out_streams[track_idx].clone(),
        })
        .collect();
    let independent_track_sinks = match sink.open_track_sinks(&track_infos, cancellation.clone()) {
        Ok(sinks) => sinks,
        Err(e) => {
            return Err(fail_sink(&mut sink, attribute(FailureStage::Sink, None)(e)));
        }
    };
    if let Some(track_sinks) = &independent_track_sinks {
        if track_sinks.len() != pipelines.len() {
            return Err(fail_sink(
                &mut sink,
                StageFailure::new(
                    FailureStage::Sink,
                    None,
                    Error::invalid(format!(
                        "pipeline: JobSink returned {} TrackSinks for {} pipeline tracks",
                        track_sinks.len(),
                        pipelines.len()
                    )),
                ),
            ));
        }
        if pipelines
            .iter()
            .any(|pipeline| !pipeline.extra_output_streams.is_empty())
        {
            return Err(fail_sink(
                &mut sink,
                StageFailure::new(
                    FailureStage::Sink,
                    None,
                    Error::unsupported(
                        "pipeline: independent TrackSinks do not yet support multi-port filter extras",
                    ),
                ),
            ));
        }
    }
    let independent_delivery = independent_track_sinks.is_some();

    for pipeline in &mut pipelines {
        if let Some(decoder) = pipeline.decoder.as_mut() {
            decoder.set_cancellation_token(cancellation.clone());
        }
    }
    let counters = Arc::new(PipelineCounters::default());
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    let progress_tx = control.progress_tx;
    let mut seek_rx = control.seek_rx;
    let (pkt_cap, frame_cap) = control.caps.unwrap_or_default().resolved();
    // Shared byte ceiling on the demuxer→worker packet queues. `0`
    // (default) is a no-op: `admit` / `release` short-circuit and the
    // demuxer never parks, so the count caps alone govern.
    let budget = QueueBudget::new(control.max_queue_bytes);
    let started_at = Instant::now();

    // Aggregate sinks retain the historical final per-track output channels
    // consumed by the central mux loop. Independent TrackSinks bypass those
    // channels: each TrackSink is moved directly into its track's terminal
    // worker, so there is no decoded-output queue merely to hand a final result
    // to the application.
    let mut track_output_rx: Vec<Receiver<Msg<OutputItem>>> = Vec::new();
    let mut terminals: Vec<Option<TrackTerminal>> = Vec::with_capacity(pipelines.len());
    match independent_track_sinks {
        Some(track_sinks) => {
            for (track_idx, (pipeline, track_sink)) in pipelines.iter().zip(track_sinks).enumerate()
            {
                terminals.push(Some(TrackTerminal {
                    track_index: track_idx as u32,
                    kind: pipeline.kind,
                    target: TrackTerminalTarget::Independent(track_sink),
                    abort: abort.clone(),
                }));
            }
        }
        None => {
            for (track_idx, pipeline) in pipelines.iter().enumerate() {
                let (tx, rx) = mpsc::sync_channel::<Msg<OutputItem>>(pkt_cap);
                track_output_rx.push(rx);
                terminals.push(Some(TrackTerminal {
                    track_index: track_idx as u32,
                    kind: pipeline.kind,
                    target: TrackTerminalTarget::Aggregate(tx),
                    abort: abort.clone(),
                }));
            }
        }
    }

    // Route tables: per source URI, the list of (source_stream,
    // packet_tx) pairs the demuxer / packet-pump thread fans packets
    // out to, plus the list of frame_tx senders a frame-pump thread
    // fans decoded frames out to (frame-shape sources have exactly one
    // synthetic stream, so no per-stream index is needed).
    type Route = (u32, SyncSender<Msg<Packet>>);
    let mut routes_by_uri: HashMap<String, Vec<Route>> = HashMap::new();
    let mut frame_routes_by_uri: HashMap<String, Vec<SyncSender<Msg<FrameLease>>>> = HashMap::new();

    // Build + spawn each track's stage chain. We consume the Vec so the
    // decoder/encoder/filters can be moved into worker threads.
    for (track_idx, mut pl) in pipelines.drain(..).enumerate() {
        let source_uri = pl.source_uri.clone();
        let source_stream = pl.source_stream;
        let source_is_frames = matches!(
            sources_by_uri.get(&pl.source_uri),
            Some(SourcePump::Frames { .. })
        );
        let mut terminal = Some(
            terminals[track_idx]
                .take()
                .expect("pipeline: missing terminal sink for track"),
        );
        let aggregate_out_tx = terminal.as_ref().and_then(TrackTerminal::aggregate_sender);
        let frame_stages = std::mem::take(&mut pl.frame_stages);
        let encoder = pl.encoder.take();
        let decoder_is_terminal = frame_stages.is_empty() && encoder.is_none();

        // Head of this track's frame chain. Packet-producing sources wire a
        // packet queue to copy/decode. Frame-shape sources require a frame
        // queue because one shared source worker may fan out to several tracks.
        let frame_head_rx: Receiver<Msg<FrameLease>> = if source_is_frames {
            if pl.copy {
                return Err(fail_sink(
                    &mut sink,
                    StageFailure::new(
                        FailureStage::Prepare,
                        Some(track_idx as u32),
                        Error::other(
                            "pipeline: copy track over a frame-shape source is not \
                             representable (frames carry no packets to copy)",
                        ),
                    ),
                ));
            }
            debug_assert!(
                pl.decoder.is_none(),
                "frame-shape track should not have instantiated a decoder"
            );
            let (frame0_tx, frame0_rx) = mpsc::sync_channel::<Msg<FrameLease>>(frame_cap);
            frame_routes_by_uri
                .entry(source_uri)
                .or_default()
                .push(frame0_tx);
            frame0_rx
        } else {
            let (pkt_tx, pkt_rx) = mpsc::sync_channel::<Msg<Packet>>(pkt_cap);
            routes_by_uri
                .entry(source_uri)
                .or_default()
                .push((source_stream, pkt_tx));

            if pl.copy {
                let abort_c = abort.clone();
                let counters_c = counters.clone();
                let budget_c = budget.clone();
                let name = format!("copy-{track_idx}");
                let stage_track = Some(track_idx as u32);
                let terminal = terminal.take().expect("copy track terminal");
                handles.push(spawn_stage(
                    abort_c,
                    name,
                    FailureStage::Copy,
                    stage_track,
                    move |abort| run_copy_stage(pkt_rx, terminal, abort, counters_c, budget_c),
                ));
                continue;
            }

            let stream_update = decoder_is_terminal.then(|| out_streams[track_idx].clone());
            let decoder = match pl.decoder.take() {
                Some(d) => d,
                None => {
                    return Err(fail_sink(
                        &mut sink,
                        StageFailure::new(
                            FailureStage::Prepare,
                            Some(track_idx as u32),
                            Error::other(
                                "pipeline: non-copy track without a decoder is not supported",
                            ),
                        ),
                    ));
                }
            };
            let (downstream, frame0_rx) = if decoder_is_terminal {
                (
                    FrameDownstream::Terminal(
                        terminal.take().expect("direct decode track terminal"),
                    ),
                    None,
                )
            } else {
                let (frame0_tx, frame0_rx) = mpsc::sync_channel::<Msg<FrameLease>>(frame_cap);
                (FrameDownstream::Channel(frame0_tx), Some(frame0_rx))
            };
            let abort_d = abort.clone();
            let counters_d = counters.clone();
            let budget_d = budget.clone();
            let name = format!("decode-{track_idx}");
            handles.push(spawn_stage(
                abort_d,
                name,
                FailureStage::Decode,
                Some(track_idx as u32),
                move |abort| {
                    run_decode_stage(
                        decoder,
                        pkt_rx,
                        downstream,
                        stream_update,
                        abort,
                        counters_d,
                        budget_d,
                    )
                },
            ));
            if decoder_is_terminal {
                continue;
            }
            frame0_rx.expect("non-terminal decoder must expose frame output")
        };

        // Count extras as we go: the first filter stage on this track starts
        // at extras_base_for_this_track, the next filter picks up where the
        // previous left off. Independent TrackSink mode rejects extras above;
        // aggregate sinks keep the historical extra-output channel.
        let extras_base_for_track: u32 = pl.extras_base_index;
        let mut running_extras_base = extras_base_for_track;
        let extra_port_counts: Vec<u32> = pl.extra_output_port_counts.clone().into_iter().collect();
        let mut extra_counts_iter = extra_port_counts.into_iter();

        let stage_count = frame_stages.len();
        let mut upstream = Some(frame_head_rx);
        let mut terminal_consumed_by_stage = false;
        for (fidx, stage) in frame_stages.into_iter().enumerate() {
            let stage_kind = frame_stage_failure_kind(&stage);
            let label = match &stage {
                FrameStage::Filter(_) => "filter",
                FrameStage::PixConvert { .. } => "convert",
            };
            let name = format!("{label}-{track_idx}-{fidx}");
            let abort_f = abort.clone();

            let (stage_extras_tx, stage_extras_base) = if matches!(stage, FrameStage::Filter(_)) {
                match extra_counts_iter.next() {
                    Some(n) if n > 0 => {
                        let base = running_extras_base;
                        running_extras_base += n;
                        (aggregate_out_tx.clone(), base)
                    }
                    _ => (None, 0),
                }
            } else {
                (None, 0)
            };

            let is_terminal_stage = encoder.is_none() && fidx + 1 == stage_count;
            let (downstream, next_rx) = if is_terminal_stage {
                (
                    FrameDownstream::Terminal(terminal.take().expect("last frame stage terminal")),
                    None,
                )
            } else {
                let (ftx, frx) = mpsc::sync_channel::<Msg<FrameLease>>(frame_cap);
                (FrameDownstream::Channel(ftx), Some(frx))
            };
            let stage_rx = upstream.take().expect("frame stage input receiver");

            let counters_f = counters.clone();
            handles.push(spawn_stage(
                abort_f,
                name,
                stage_kind,
                Some(track_idx as u32),
                move |abort| {
                    run_frame_stage_worker(
                        stage,
                        stage_rx,
                        downstream,
                        stage_extras_tx,
                        stage_extras_base,
                        abort,
                        counters_f,
                    )
                },
            ));

            match next_rx {
                Some(frx) => upstream = Some(frx),
                None => {
                    terminal_consumed_by_stage = true;
                    break;
                }
            }
        }

        if let Some(enc) = encoder {
            let abort_e = abort.clone();
            let counters_e = counters.clone();
            let terminal = terminal.take().expect("encoder track terminal");
            let upstream = upstream.take().expect("encoder input receiver");
            let name = format!("encode-{track_idx}");
            handles.push(spawn_stage(
                abort_e,
                name,
                FailureStage::Encode,
                Some(track_idx as u32),
                move |abort| run_encode_stage(enc, upstream, terminal, abort, counters_e),
            ));
        } else if !terminal_consumed_by_stage {
            // Frame-shape source with no later stage: the small per-track input
            // queue is a genuine source-fanout boundary, but the terminal worker
            // delivers directly to TrackSink / aggregate output with no second
            // final-output queue.
            let abort_r = abort.clone();
            let counters_r = counters.clone();
            let terminal = terminal.take().expect("frame source track terminal");
            let upstream = upstream.take().expect("frame terminal input receiver");
            let name = format!("frame-terminal-{track_idx}");
            handles.push(spawn_stage(
                abort_r,
                name,
                FailureStage::Sink,
                Some(track_idx as u32),
                move |abort| run_frame_fanout(upstream, terminal, abort, counters_r),
            ));
        }
    }

    // Spawn one source-pump thread per URI, shaped by the source kind:
    // bytes-shape URIs get the demuxer stage, packet-shape URIs get the
    // packet-pump stage (identical fan-out, no container layer, seeks
    // rejected), frame-shape URIs get the frame-pump stage (frames fan
    // straight into the per-track frame chains).
    //
    // Seek plumbing: the handle's single seek receiver is owned by the
    // FIRST routed source pump (the "seek owner"); every OTHER routed
    // source gets a dedicated forwarding channel, and the owner
    // re-sends each drained [`SeekCmd`] to every sibling BEFORE
    // handling it locally. One dispatch therefore reaches EVERY routed
    // source, so a job whose tracks come from several URIs (separate
    // audio + video files, say) re-anchors all of them instead of
    // silently seeking only one and desyncing the rest. Each source
    // answers with its own barrier — `SeekFlush` with its landed pts,
    // or `SeekRejected` when it has no seek surface — all stamped with
    // the command's generation, so every track observes exactly one
    // barrier per dispatched generation. The forwarding senders never
    // block (unbounded channel) and a sibling that already exited just
    // drops the forwards. When the OWNER hits EOF the whole seek
    // surface winds down with it — identical to the historical
    // single-receiver lifetime.
    type RoutedSource = (
        String,
        SourcePump,
        Vec<Route>,
        Vec<SyncSender<Msg<FrameLease>>>,
    );
    let mut routed: Vec<RoutedSource> = Vec::new();
    for (uri, pump) in sources_by_uri {
        let pkt_routes = routes_by_uri.remove(&uri).unwrap_or_default();
        let frame_routes = frame_routes_by_uri.remove(&uri).unwrap_or_default();
        if pkt_routes.is_empty() && frame_routes.is_empty() {
            continue;
        }
        routed.push((uri, pump, pkt_routes, frame_routes));
    }
    let mut seek_inputs: Vec<Option<Receiver<SeekCmd>>> = Vec::with_capacity(routed.len());
    let mut owner_fanout: Vec<mpsc::Sender<SeekCmd>> = Vec::new();
    for i in 0..routed.len() {
        if i == 0 {
            seek_inputs.push(seek_rx.take());
        } else if seek_inputs[0].is_some() {
            let (tx, rx) = mpsc::channel::<SeekCmd>();
            owner_fanout.push(tx);
            seek_inputs.push(Some(rx));
        } else {
            seek_inputs.push(None);
        }
    }
    for (i, (uri, pump, pkt_routes, frame_routes)) in routed.into_iter().enumerate() {
        let abort_d = abort.clone();
        let counters_d = counters.clone();
        let budget_d = budget.clone();
        let src_seek_rx = seek_inputs[i].take();
        let seek_fanout = if i == 0 {
            std::mem::take(&mut owner_fanout)
        } else {
            Vec::new()
        };
        match pump {
            SourcePump::Demuxer(dmx) => {
                let name = format!("demux-{uri}");
                handles.push(spawn_stage(
                    abort_d,
                    name,
                    FailureStage::Source,
                    None,
                    move |abort| {
                        run_demuxer_stage(
                            dmx,
                            pkt_routes,
                            abort,
                            counters_d,
                            src_seek_rx,
                            seek_fanout,
                            budget_d,
                        )
                    },
                ));
            }
            SourcePump::Packets(src) => {
                let name = format!("packets-{uri}");
                handles.push(spawn_stage(
                    abort_d,
                    name,
                    FailureStage::Source,
                    None,
                    move |abort| {
                        run_packet_source_stage(
                            src,
                            pkt_routes,
                            abort,
                            counters_d,
                            src_seek_rx,
                            seek_fanout,
                            budget_d,
                        )
                    },
                ));
            }
            SourcePump::Frames { source, .. } => {
                let name = format!("frames-{uri}");
                handles.push(spawn_stage(
                    abort_d,
                    name,
                    FailureStage::Source,
                    None,
                    move |abort| {
                        run_frame_source_stage(
                            source,
                            frame_routes,
                            abort,
                            counters_d,
                            src_seek_rx,
                            seek_fanout,
                        )
                    },
                ));
            }
        }
    }

    if independent_delivery {
        // Track terminal workers own their TrackSinks directly. There is no
        // mux-end receiver to drain or drop; wait for the graph to finish.
        // On error/external stop the shared cancellation token wakes blocking
        // TrackSinks, whose exit drops their upstream receivers and lets the
        // bounded-channel backpressure unwind towards the source.
        for h in handles {
            let _ = h.join();
        }
    } else {
        // Mux loop on the caller thread — drain across every track output
        // channel until all are EOF or abort is set.
        //
        // Pre-fix this used a per-track `recv_timeout(50ms)` round-robin: when
        // one track was empty, the mux blocked 50 ms on it before checking the
        // next, even if the next had data ready. With audio + video tracks
        // running in parallel and the slower decoder running ~one frame per
        // packet, the empty-track stall throttled the *full* track to ~1
        // message per 50 ms (~20 msg/s). On `solana-ad.mp4` that surfaced as
        // audio-ring drain during real playback: `--vo winit+wgpu --ao auto`
        // saw the audio queue collapse from ~1 s to ~0 s within five seconds.
        //
        // The new shape is a non-blocking round-robin: each pass calls
        // `try_recv` on every track in turn, processing whatever is ready.
        // When *every* track is empty AND none have disconnected, park briefly
        // (1 ms) so we don't spin a CPU. EOF and disconnection are still
        // counted as terminal exactly as before. This keeps fast-track
        // throughput bounded only by the receive + sink-write cost, not by
        // any sibling track's idleness.
        let mut eof_state: Vec<bool> = vec![false; track_output_rx.len()];
        let mut eof_count = 0usize;
        let total = track_output_rx.len();
        while eof_count < total {
            if abort.is_aborted() {
                break;
            }
            let mut made_progress = false;
            for i in 0..total {
                if eof_state[i] {
                    continue;
                }
                let rx = &track_output_rx[i];
                match rx.try_recv() {
                    Ok(Msg::Data(item)) => {
                        made_progress = true;
                        let pts = match &item.payload {
                            OutputPayload::Packet(p) => p.pts,
                            OutputPayload::Frame(f) => f.pts(),
                        };
                        match item.payload {
                            OutputPayload::Packet(mut p) => {
                                p.stream_index = item.track_index;
                                if let Err(e) = sink.write_packet(item.kind, &p) {
                                    abort.record_failure(StageFailure::new(
                                        FailureStage::Sink,
                                        Some(item.track_index),
                                        e,
                                    ));
                                    break;
                                }
                            }
                            OutputPayload::Frame(f) => {
                                if let Err(e) = sink.write_frame_lease(item.kind, f) {
                                    abort.record_failure(StageFailure::new(
                                        FailureStage::Sink,
                                        Some(item.track_index),
                                        e,
                                    ));
                                    break;
                                }
                                counters.frames_written.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        if let Some(tx) = &progress_tx {
                            let frames = counters.frames_written.load(Ordering::SeqCst);
                            let skipped = counters.packets_skipped.load(Ordering::SeqCst);
                            let read = counters.packets_read.load(Ordering::SeqCst);
                            let encoded = counters.packets_encoded.load(Ordering::SeqCst);
                            let copied = counters.packets_copied.load(Ordering::SeqCst);
                            let _ = tx.try_send(Progress {
                                pts,
                                frames,
                                eof: false,
                                queue_bytes: budget.in_flight(),
                                elapsed_micros: started_at.elapsed().as_micros() as u64,
                                packets_skipped: skipped,
                                packets_read: read,
                                packets_encoded: encoded,
                                packets_copied: copied,
                            });
                        }
                    }
                    Ok(Msg::StreamUpdate(stream)) => {
                        made_progress = true;
                        if let Err(e) = sink.stream_update(&stream) {
                            abort.record_failure(StageFailure::new(
                                FailureStage::Sink,
                                Some(stream.index),
                                e,
                            ));
                            break;
                        }
                    }
                    Ok(Msg::Barrier(kind)) => {
                        made_progress = true;
                        if let Err(e) = sink.barrier(kind) {
                            abort.record_failure(StageFailure::new(
                                FailureStage::Sink,
                                Some(i as u32),
                                e,
                            ));
                            break;
                        }
                    }
                    Ok(Msg::Eof) => {
                        made_progress = true;
                        if !eof_state[i] {
                            eof_state[i] = true;
                            eof_count += 1;
                        }
                    }
                    Err(mpsc::TryRecvError::Empty) => {
                        // Try the next track; if all are empty we'll park
                        // briefly below.
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        // Producer panicked or exited without sending Eof —
                        // count as EOF to avoid hanging. Any error was
                        // already recorded on the abort state.
                        if !eof_state[i] {
                            eof_state[i] = true;
                            eof_count += 1;
                        }
                    }
                }
            }
            if !made_progress && eof_count < total {
                // Every track was empty this pass — park 1 ms so we don't
                // spin a CPU core while waiting for upstream stages.
                thread::sleep(Duration::from_millis(1));
            }
        }

        // Drain abort flag + wait for workers regardless of exit path.
        abort.request_abort();
        // Drop the mux-end receivers BEFORE joining workers. Upstream
        // stages (copy / decode / filter / pix-convert / demux) may be
        // blocked inside `SyncSender::send()` because the bounded
        // channel is full — setting the abort flag alone doesn't wake
        // them. Dropping the receivers turns every pending send into an
        // `Err(SendError)`, the worker's `tx.send().is_err()` branch
        // breaks its loop, and the cascade propagates up to the demuxer.
        // Without this, `h.join()` below deadlocks on any abort-path
        // exit (quit event, sink error, encoder fail).
        drop(track_output_rx);
        for h in handles {
            let _ = h.join();
        }
    }
    if let Some(mut failure) = abort.take_failure() {
        // Attach the partial counters: workers have joined, so the
        // snapshot reflects everything that actually reached the sink
        // before teardown (`RunFailure::stats`).
        failure.stats = counters.snapshot();
        return Err(fail_sink(&mut sink, failure));
    }
    if let Err(e) = sink.finish() {
        let mut failure = attribute(FailureStage::SinkFinish, None)(e);
        failure.stats = counters.snapshot();
        return Err(fail_sink(&mut sink, failure));
    }
    if let Some(tx) = &progress_tx {
        let frames = counters.frames_written.load(Ordering::SeqCst);
        // At EOF the demuxer has drained all packets and every consuming
        // stage has released its bytes, so `in_flight()` should be 0 —
        // but we read it rather than hard-code 0 so a late drain race
        // reports the actual observable value instead of lying.
        //
        // `elapsed_micros` here is the total wall-clock the pipelined run
        // took, from the baseline taken just before workers spawned through
        // to `sink.finish()` returning. CLI tools that wrap a transcode in
        // a status line can read this off the EOF progress event instead of
        // bracketing `executor.spawn()/.stop()` with their own `Instant`.
        let skipped = counters.packets_skipped.load(Ordering::SeqCst);
        let read = counters.packets_read.load(Ordering::SeqCst);
        let encoded = counters.packets_encoded.load(Ordering::SeqCst);
        let copied = counters.packets_copied.load(Ordering::SeqCst);
        // EOF emission. We want this event to reach the receiver
        // reliably — engines rely on the final `packets_read` /
        // `packets_skipped` / `packets_encoded` / wall-clock totals it
        // carries — but a blocking `send` would deadlock if the engine
        // never polled progress, which is fine for handles that opt
        // out of progress observation entirely (e.g.
        // `tests/seek_with_generation.rs`). Strategy: try a bounded
        // number of `try_send` attempts with a short park between,
        // giving any backed-up receiver a window to drain. Drop on
        // saturation rather than wait forever — the engine still
        // observes completion via `has_finished()` even if this exact
        // event was lost on a full channel.
        let eof_evt = Progress {
            pts: None,
            frames,
            eof: true,
            queue_bytes: budget.in_flight(),
            elapsed_micros: started_at.elapsed().as_micros() as u64,
            packets_skipped: skipped,
            packets_read: read,
            packets_encoded: encoded,
            packets_copied: copied,
        };
        for attempt in 0..100 {
            match tx.try_send(eof_evt) {
                Ok(_) => break,
                Err(mpsc::TrySendError::Full(_)) => {
                    if attempt == 99 {
                        // 100 ms total — give up and let the engine
                        // observe completion via `has_finished()` only.
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                Err(mpsc::TrySendError::Disconnected(_)) => break,
            }
        }
    }
    Ok(counters.snapshot())
}

/// Spawn a worker thread that runs `work` under `abort`. If `work`
/// returns `Err`, record it on `abort` (first-wins) with the given
/// `(stage, track)` attribution and flip the abort flag so peers can
/// bail.
fn spawn_stage<F>(
    abort: Arc<AbortState>,
    name: String,
    stage: FailureStage,
    track: Option<u32>,
    work: F,
) -> JoinHandle<()>
where
    F: FnOnce(Arc<AbortState>) -> Result<()> + Send + 'static,
{
    thread::Builder::new()
        .name(format!("oxideav-job:{name}"))
        .spawn(move || {
            if let Err(e) = work(abort.clone()) {
                abort.record_failure(StageFailure::new(stage, track, e));
            }
        })
        .expect("pipeline: thread spawn")
}

// ───────────────────────── stage workers ─────────────────────────

/// Demuxer thread: read packets until EOF, fan out to each route whose
/// source_stream matches. Broadcasts `Msg::Eof` to every route on EOF.
///
/// Optional `seek_rx` carries [`SeekCmd`]s from
/// [`crate::ExecutorHandle::seek`]. On each iteration we
/// non-blocking-poll the channel; on a SeekCmd we bump `generation`,
/// call `dmx.seek_to`, and fan a single barrier out on every route:
/// [`BarrierKind::SeekFlush`] on success (workers drop in-flight
/// state) or [`BarrierKind::SeekRejected`] on error (workers leave
/// state alone; the demuxer keeps reading from its prior position
/// so the pipeline stays alive). The barrier lands on the mux loop,
/// which calls `sink.barrier(kind)`.
///
/// Rejecting a seek is NOT a fatal pipeline error — pre-fix, the
/// stage propagated `seek_to`'s error and the entire executor died
/// the first time a user pressed `→` on a stream backed by a
/// demuxer whose `seek_to` was the default `Error::unsupported`.
fn run_demuxer_stage(
    mut dmx: Box<dyn Demuxer>,
    routes: Vec<(u32, SyncSender<Msg<Packet>>)>,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
    seek_rx: Option<Receiver<SeekCmd>>,
    seek_fanout: Vec<mpsc::Sender<SeekCmd>>,
    budget: Arc<QueueBudget>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        // Memory-bounded back-pressure: hold off reading the next packet
        // while the in-flight packet bytes are at or above the ceiling.
        // A no-op when no `max_queue_bytes` was set. This sits BEFORE the
        // seek drain so a parked demuxer still wakes promptly on abort
        // (the wait itself bails on the abort flag).
        budget.wait_below_ceiling(&abort);
        if abort.is_aborted() {
            break;
        }
        // Drain any pending seeks before reading the next packet. We
        // ask the demuxer to seek FIRST and broadcast the matching
        // barrier AFTER, so workers see whether the seek landed
        // (`SeekFlush` — reset codec state) or was rejected
        // (`SeekRejected` — keep going from the prior position).
        //
        // Pre-fix this loop broadcast `SeekFlush` unconditionally and
        // then propagated any `seek_to` error via `return Err(e)`,
        // which killed the entire demuxer thread the first time a
        // user pressed `→` on a stream backed by a demuxer whose
        // `seek_to` was the default `Error::unsupported`. The
        // executor would surface the error, the engine would stall
        // with no further packets, and the player UI froze. We now
        // keep the pipeline alive on rejection and signal the engine
        // via a dedicated barrier kind so it can disable seek UI for
        // the session.
        //
        // Generation comes from the caller (`cmd.generation`, assigned
        // by `ExecutorHandle::seek_with_generation`'s atomic counter)
        // rather than a local counter, so the handle's returned value
        // and the resulting barrier's `generation` are guaranteed to
        // match in lockstep regardless of how many seeks are queued.
        if let Some(rx) = &seek_rx {
            while let Ok(cmd) = rx.try_recv() {
                // Seek-owner duty: forward the command to every sibling
                // routed source BEFORE handling it locally, so a
                // multi-URI job re-anchors all of its sources on one
                // dispatch. Non-owners have an empty `seek_fanout`.
                for tx in &seek_fanout {
                    let _ = tx.send(cmd);
                }
                // A command addressing a stream this source doesn't
                // route (the primary target lives on a sibling URI)
                // still seeks THIS source — retargeted at its first
                // routed stream with the pts rescaled into that
                // stream's time base — so all sources land on the same
                // presentation instant.
                let (dst_stream, dst_pts, dst_tb) =
                    resolve_seek_target(&routes, dmx.streams(), &cmd);
                let kind = match dmx.seek_to(dst_stream, dst_pts) {
                    Ok(landed_pts) => BarrierKind::SeekFlush {
                        generation: cmd.generation,
                        landed_pts,
                        time_base: dst_tb,
                    },
                    Err(_e) => BarrierKind::SeekRejected {
                        generation: cmd.generation,
                    },
                };
                for (_, tx) in &routes {
                    if tx.send(Msg::Barrier(kind)).is_err() {
                        abort.request_abort();
                        return Ok(());
                    }
                }
            }
        }
        match dmx.next_packet() {
            Ok(pkt) => {
                counters.packets_read.fetch_add(1, Ordering::SeqCst);
                let bytes = pkt.data.len() as u64;
                for (stream_idx, tx) in &routes {
                    if *stream_idx != pkt.stream_index {
                        continue;
                    }
                    // Account this copy's bytes as in-flight BEFORE the
                    // send so the running total never undershoots what's
                    // physically queued. The consuming stage releases the
                    // same count when it pulls the packet off the channel.
                    // Each matched route gets its own `pkt.clone()`, hence
                    // its own admit/release pair.
                    budget.admit(bytes);
                    if tx.send(Msg::Data(pkt.clone())).is_err() {
                        // Consumer gone; likely aborted. The packet never
                        // reached a receiver, so the consumer will never
                        // release it — undo the admit here.
                        budget.release(bytes);
                        abort.request_abort();
                        break;
                    }
                }
            }
            Err(Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    for (_, tx) in routes {
        let _ = tx.send(Msg::Eof);
    }
    Ok(())
}

/// Packet-source thread: like [`run_demuxer_stage`] but drives a
/// [`PacketSource`] (RTMP, future SRT / RTSP, …) — same per-stream
/// fan-out, same byte-budget accounting, no container layer.
///
/// Packet sources may optionally implement [`PacketSource::seek_to`]. A
/// successful seek emits [`BarrierKind::SeekFlush`] so downstream workers
/// reset codec/filter state; an unsupported/rejected seek emits
/// [`BarrierKind::SeekRejected`] and leaves the source running from its prior
/// position. This mirrors the demuxer-source control path.
/// Resolve which stream of THIS source a [`SeekCmd`] should move, and
/// to what pts.
///
/// * The command's `stream_idx` is one of this source's routed streams
///   → primary target: seek it with the command's pts verbatim
///   (the historical single-source behaviour).
/// * Otherwise the primary target lives on a sibling source of a
///   multi-URI job, and this source is being re-anchored to the same
///   presentation instant: retarget at the FIRST routed stream, with
///   the pts rescaled from the command's time base into that stream's
///   own — the returned time base is the one the eventual
///   `SeekFlush::landed_pts` is expressed in, keeping the barrier's
///   "landed pts with matching time_base" contract intact. When the
///   stream's info is unavailable the pts passes through unscaled
///   (best effort; the demuxer clamps).
///
/// The generic `T` is the route payload (a channel sender at the call
/// site); only the stream index half of each route matters here.
fn resolve_seek_target<T>(
    routes: &[(u32, T)],
    streams: &[StreamInfo],
    cmd: &SeekCmd,
) -> (u32, i64, TimeBase) {
    if routes.iter().any(|(s, _)| *s == cmd.stream_idx) {
        return (cmd.stream_idx, cmd.pts, cmd.time_base);
    }
    let dst = routes.first().map(|(s, _)| *s).unwrap_or(cmd.stream_idx);
    match streams.iter().find(|s| s.index == dst) {
        Some(info) => (
            dst,
            cmd.time_base.rescale(cmd.pts, info.time_base),
            info.time_base,
        ),
        None => (dst, cmd.pts, cmd.time_base),
    }
}

fn run_packet_source_stage(
    mut src: Box<dyn PacketSource>,
    routes: Vec<(u32, SyncSender<Msg<Packet>>)>,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
    seek_rx: Option<Receiver<SeekCmd>>,
    seek_fanout: Vec<mpsc::Sender<SeekCmd>>,
    budget: Arc<QueueBudget>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        budget.wait_below_ceiling(&abort);
        if abort.is_aborted() {
            break;
        }
        if let Some(rx) = &seek_rx {
            while let Ok(cmd) = rx.try_recv() {
                // Seek-owner duty (see `run_demuxer_stage`): a packet
                // source can own the receiver in a mixed-shape multi-URI
                // job, so siblings receive the same command first.
                for tx in &seek_fanout {
                    let _ = tx.send(cmd);
                }
                let (dst_stream, dst_pts, dst_tb) =
                    resolve_seek_target(&routes, src.streams(), &cmd);
                let kind = match src.seek_to(dst_stream, dst_pts) {
                    Ok(landed_pts) => BarrierKind::SeekFlush {
                        generation: cmd.generation,
                        landed_pts,
                        time_base: dst_tb,
                    },
                    Err(_e) => BarrierKind::SeekRejected {
                        generation: cmd.generation,
                    },
                };
                for (_, tx) in &routes {
                    if tx.send(Msg::Barrier(kind)).is_err() {
                        abort.request_abort();
                        return Ok(());
                    }
                }
            }
        }
        match src.next_packet() {
            Ok(pkt) => {
                counters.packets_read.fetch_add(1, Ordering::SeqCst);
                let bytes = pkt.data.len() as u64;
                for (stream_idx, tx) in &routes {
                    if *stream_idx != pkt.stream_index {
                        continue;
                    }
                    budget.admit(bytes);
                    if tx.send(Msg::Data(pkt.clone())).is_err() {
                        budget.release(bytes);
                        abort.request_abort();
                        break;
                    }
                }
            }
            Err(Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    for (_, tx) in routes {
        let _ = tx.send(Msg::Eof);
    }
    Ok(())
}

/// Frame-source thread: drives a [`FrameSource`] (synthetic generator,
/// future capture-card driver, rendered 3D scene) and fans each frame
/// out to every consuming track's frame chain — no demux stage, no
/// decode stage. Mirrors the serial path's multi-consumer clone
/// semantics: one `next_frame()` call per source frame, cloned per
/// additional consumer; `frames_decoded` counts source frames once,
/// matching [`crate::executor::ExecutorStats`] parity with the serial
/// runner.
///
/// [`FrameSource`] has no seek surface, so every [`SeekCmd`] is
/// answered with a [`BarrierKind::SeekRejected`] carrying the
/// command's generation (see [`run_packet_source_stage`]).
fn run_frame_source_stage(
    mut src: Box<dyn FrameSource>,
    routes: Vec<SyncSender<Msg<FrameLease>>>,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
    seek_rx: Option<Receiver<SeekCmd>>,
    seek_fanout: Vec<mpsc::Sender<SeekCmd>>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        if let Some(rx) = &seek_rx {
            while let Ok(cmd) = rx.try_recv() {
                // Seek-owner duty (see `run_demuxer_stage`): forward
                // to siblings even though a frame source itself has no
                // seek surface.
                for tx in &seek_fanout {
                    let _ = tx.send(cmd);
                }
                let kind = BarrierKind::SeekRejected {
                    generation: cmd.generation,
                };
                for tx in &routes {
                    if tx.send(Msg::Barrier(kind)).is_err() {
                        abort.request_abort();
                        return Ok(());
                    }
                }
            }
        }
        match src.next_frame() {
            Ok(frame) => {
                counters.frames_decoded.fetch_add(1, Ordering::SeqCst);
                let lease = FrameLease::from_frame(frame);
                for tx in &routes {
                    if tx.send(Msg::Data(lease.clone())).is_err() {
                        abort.request_abort();
                        break;
                    }
                }
            }
            Err(Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    for tx in routes {
        let _ = tx.send(Msg::Eof);
    }
    Ok(())
}

/// Copy track: packets straight to its terminal sink.
fn run_copy_stage(
    rx: Receiver<Msg<Packet>>,
    mut terminal: TrackTerminal,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
    budget: Arc<QueueBudget>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(pkt)) => {
                budget.release(pkt.data.len() as u64);
                match terminal.write_packet(pkt) {
                    Ok(DeliveryStatus::Delivered) => {
                        counters.packets_copied.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(DeliveryStatus::Closed) => {
                        abort.request_abort();
                        break;
                    }
                    Err(e) if e.is_cancelled() && abort.is_aborted() => break,
                    Err(e) => return Err(e),
                }
            }
            Ok(Msg::StreamUpdate(_)) => {}
            Ok(Msg::Barrier(b)) => match terminal.barrier(b) {
                Ok(DeliveryStatus::Delivered) => {}
                Ok(DeliveryStatus::Closed) => {
                    abort.request_abort();
                    break;
                }
                Err(e) if e.is_cancelled() && abort.is_aborted() => break,
                Err(e) => return Err(e),
            },
            Ok(Msg::Eof) | Err(_) => break,
        }
    }
    terminal.eof();
    Ok(())
}
/// Decoder stage: packets -> frames or directly into the track terminal.
fn run_decode_stage(
    mut decoder: Box<dyn Decoder>,
    rx: Receiver<Msg<Packet>>,
    mut downstream: FrameDownstream,
    mut stream_template: Option<StreamInfo>,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
    budget: Arc<QueueBudget>,
) -> Result<()> {
    let mut last_stream_params: Option<CodecParameters> = None;
    'outer: loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(pkt)) => {
                budget.release(pkt.data.len() as u64);
                if let Err(e) = decoder.send_packet(&pkt) {
                    if e.is_cancelled() && abort.is_aborted() {
                        break 'outer;
                    }
                    if e.is_cancelled() || e.is_resource_exhausted() {
                        return Err(e);
                    }
                    counters.packets_skipped.fetch_add(1, Ordering::SeqCst);
                    eprintln!(
                        "pipeline: decoder skipped packet (stream {}, pts {:?}): {}",
                        pkt.stream_index, pkt.pts, e
                    );
                    continue;
                }

                if let (Some(template), Some(params)) =
                    (stream_template.as_mut(), decoder.output_params())
                {
                    let changed = last_stream_params
                        .as_ref()
                        .map_or(true, |last| !last.matches_core(params));
                    if changed {
                        template.params = params.clone();
                        if !deliver_stream_update(&mut downstream, template.clone(), &abort)? {
                            break 'outer;
                        }
                        last_stream_params = Some(params.clone());
                    }
                }

                let mut produced_any = false;
                loop {
                    if abort.is_aborted() {
                        break 'outer;
                    }
                    match decoder.receive_frame_lease() {
                        Ok(frame) => {
                            counters.frames_decoded.fetch_add(1, Ordering::SeqCst);
                            produced_any = true;
                            if !deliver_frame_downstream(&mut downstream, frame, &abort, &counters)?
                            {
                                break 'outer;
                            }
                        }
                        Err(Error::NeedMore) => break,
                        Err(Error::Eof) => break 'outer,
                        Err(e) => {
                            if !produced_any {
                                counters.packets_skipped.fetch_add(1, Ordering::SeqCst);
                            }
                            eprintln!(
                                "pipeline: decoder skipped frame after packet (stream {}, pts {:?}): {}",
                                pkt.stream_index, pkt.pts, e
                            );
                            break;
                        }
                    }
                }
            }
            Ok(Msg::StreamUpdate(_)) => {}
            Ok(Msg::Barrier(b)) => {
                if matches!(b, BarrierKind::SeekFlush { .. }) {
                    let _ = decoder.reset();
                }
                if !deliver_frame_barrier(&mut downstream, b, &abort)? {
                    break;
                }
            }
            Ok(Msg::Eof) => {
                if let Err(e) = decoder.flush() {
                    eprintln!("pipeline: decoder flush error: {}", e);
                }
                loop {
                    if abort.is_aborted() {
                        break 'outer;
                    }
                    match decoder.receive_frame_lease() {
                        Ok(frame) => {
                            counters.frames_decoded.fetch_add(1, Ordering::SeqCst);
                            if !deliver_frame_downstream(&mut downstream, frame, &abort, &counters)?
                            {
                                break 'outer;
                            }
                        }
                        Err(Error::NeedMore) | Err(Error::Eof) => break,
                        Err(e) => {
                            eprintln!("pipeline: decoder error during EOF drain: {}", e);
                            break;
                        }
                    }
                }
                break;
            }
            Err(_) => break,
        }
    }
    downstream.eof();
    Ok(())
}

/// Frame-stage worker: consumes frames, runs them through an audio
/// filter or pixel-format conversion, and forwards to the next stage or,
/// when terminal, directly to the track sink.
fn run_frame_stage_worker(
    mut stage: FrameStage,
    rx: Receiver<Msg<FrameLease>>,
    mut downstream: FrameDownstream,
    extras_tx: Option<SyncSender<Msg<OutputItem>>>,
    extras_base: u32,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(lease)) => {
                let frame = lease.into_frame()?;
                let emissions = run_frame_stage_emit(&mut stage, frame)?;
                dispatch_extras(emissions.extras, &extras_tx, extras_base, &abort);
                for frame in emissions.primary {
                    if !deliver_frame_downstream(
                        &mut downstream,
                        FrameLease::from_frame(frame),
                        &abort,
                        &counters,
                    )? {
                        break;
                    }
                }
            }
            Ok(Msg::StreamUpdate(_)) => {}
            Ok(Msg::Barrier(b)) => {
                if matches!(b, BarrierKind::SeekFlush { .. }) {
                    reset_frame_stage(&mut stage);
                }
                if let Some(etx) = &extras_tx {
                    let _ = etx.send(Msg::Barrier(b));
                }
                if !deliver_frame_barrier(&mut downstream, b, &abort)? {
                    break;
                }
            }
            Ok(Msg::Eof) => {
                let emissions = flush_frame_stage_emit(&mut stage)?;
                dispatch_extras(emissions.extras, &extras_tx, extras_base, &abort);
                for frame in emissions.primary {
                    if !deliver_frame_downstream(
                        &mut downstream,
                        FrameLease::from_frame(frame),
                        &abort,
                        &counters,
                    )? {
                        break;
                    }
                }
                break;
            }
            Err(_) => break,
        }
    }
    downstream.eof();
    Ok(())
}

/// Drop internal state of a [`FrameStage`] on a `SeekFlush` barrier.
/// Filters delegate to [`oxideav_core::StreamFilter::reset`] (default no-op);
/// pixel-format converts hold no state.
fn reset_frame_stage(stage: &mut FrameStage) {
    match stage {
        FrameStage::Filter(f) => {
            let _ = f.inner.reset();
        }
        FrameStage::PixConvert { .. } => {}
    }
}

/// Push extra filter emissions onto the sink's output channel (if present).
/// Extras are tagged with indices starting at `extras_base`; port 1
/// becomes `extras_base`, port 2 `extras_base + 1`, etc.
fn dispatch_extras(
    extras: Vec<(MediaType, Frame)>,
    extras_tx: &Option<SyncSender<Msg<OutputItem>>>,
    extras_base: u32,
    abort: &Arc<AbortState>,
) {
    let Some(tx) = extras_tx else {
        return;
    };
    // The extras vec carries entries in port-1,2,3,… order as emitted
    // by the filter, but a single `push` may emit multiple frames per
    // port. We can't recover the port number from the (kind, frame)
    // tuple alone, so we tag every extra with `extras_base` + its
    // media-kind slot. For the single-extra-port case (spectrogram)
    // this is equivalent to `extras_base`.
    for (kind, frame) in extras {
        let item = OutputItem {
            track_index: extras_base,
            kind,
            payload: OutputPayload::Frame(FrameLease::from_frame(frame)),
        };
        if tx.send(Msg::Data(item)).is_err() {
            abort.request_abort();
            return;
        }
    }
}

/// Encoder stage: frames -> packets -> terminal sink.
fn run_encode_stage(
    mut encoder: Box<dyn Encoder>,
    rx: Receiver<Msg<FrameLease>>,
    mut terminal: TrackTerminal,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(lease)) => {
                let frame = lease.into_frame()?;
                encoder.send_frame(&frame)?;
                if !drain_and_send(encoder.as_mut(), &mut terminal, &abort, &counters)? {
                    break;
                }
            }
            Ok(Msg::StreamUpdate(_)) => {}
            Ok(Msg::Barrier(b)) => {
                if matches!(b, BarrierKind::SeekFlush { .. }) {
                    let _ = encoder.flush();
                    if !drain_and_send(encoder.as_mut(), &mut terminal, &abort, &counters)? {
                        break;
                    }
                }
                match terminal.barrier(b) {
                    Ok(DeliveryStatus::Delivered) => {}
                    Ok(DeliveryStatus::Closed) => {
                        abort.request_abort();
                        break;
                    }
                    Err(e) if e.is_cancelled() && abort.is_aborted() => break,
                    Err(e) => return Err(e),
                }
            }
            Ok(Msg::Eof) => {
                encoder.flush()?;
                let _ = drain_and_send(encoder.as_mut(), &mut terminal, &abort, &counters)?;
                break;
            }
            Err(_) => break,
        }
    }
    terminal.eof();
    Ok(())
}

/// Terminal worker for a frame-shape source with no later processing stage.
fn run_frame_fanout(
    rx: Receiver<Msg<FrameLease>>,
    terminal: TrackTerminal,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
) -> Result<()> {
    let mut downstream = FrameDownstream::Terminal(terminal);
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(frame)) => {
                if !deliver_frame_downstream(&mut downstream, frame, &abort, &counters)? {
                    break;
                }
            }
            Ok(Msg::StreamUpdate(stream)) => {
                if !deliver_stream_update(&mut downstream, *stream, &abort)? {
                    break;
                }
            }
            Ok(Msg::Barrier(b)) => {
                if !deliver_frame_barrier(&mut downstream, b, &abort)? {
                    break;
                }
            }
            Ok(Msg::Eof) | Err(_) => break,
        }
    }
    downstream.eof();
    Ok(())
}

fn drain_and_send(
    encoder: &mut dyn Encoder,
    terminal: &mut TrackTerminal,
    abort: &Arc<AbortState>,
    counters: &PipelineCounters,
) -> Result<bool> {
    loop {
        match encoder.receive_packet() {
            Ok(packet) => match terminal.write_packet(packet) {
                Ok(DeliveryStatus::Delivered) => {
                    counters.packets_encoded.fetch_add(1, Ordering::SeqCst);
                }
                Ok(DeliveryStatus::Closed) => {
                    abort.request_abort();
                    return Ok(false);
                }
                Err(e) if e.is_cancelled() && abort.is_aborted() => return Ok(false),
                Err(e) => return Err(e),
            },
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(true),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_info(index: u32, tb: TimeBase) -> StreamInfo {
        StreamInfo {
            index,
            time_base: tb,
            duration: None,
            start_time: Some(0),
            params: oxideav_core::CodecParameters::audio(oxideav_core::CodecId::new("t")),
        }
    }

    fn cmd(stream_idx: u32, pts: i64, tb: TimeBase) -> SeekCmd {
        SeekCmd {
            stream_idx,
            pts,
            time_base: tb,
            generation: 7,
        }
    }

    #[test]
    fn seek_target_primary_stream_passes_through_verbatim() {
        // The command addresses a routed stream → identical target,
        // pts, and time base (the historical single-source path).
        let routes: Vec<(u32, ())> = vec![(0, ()), (2, ())];
        let streams = [
            stream_info(0, TimeBase::new(1, 8_000)),
            stream_info(2, TimeBase::new(1, 90_000)),
        ];
        let c = cmd(2, 12_345, TimeBase::new(1, 90_000));
        let (s, p, tb) = resolve_seek_target(&routes, &streams, &c);
        assert_eq!(s, 2);
        assert_eq!(p, 12_345);
        assert_eq!(tb, TimeBase::new(1, 90_000));
    }

    #[test]
    fn seek_target_foreign_stream_rescales_to_first_route() {
        // The command addresses stream 5 (a sibling source's stream);
        // this source routes stream 1 at 90 kHz. 30 s expressed in
        // 1/8000 ticks (240_000) must become 30 s in 1/90000 ticks
        // (2_700_000), and the returned time base must be the one the
        // landed pts will be expressed in.
        let routes: Vec<(u32, ())> = vec![(1, ())];
        let streams = [
            stream_info(0, TimeBase::new(1, 8_000)),
            stream_info(1, TimeBase::new(1, 90_000)),
        ];
        let c = cmd(5, 240_000, TimeBase::new(1, 8_000));
        let (s, p, tb) = resolve_seek_target(&routes, &streams, &c);
        assert_eq!(s, 1);
        assert_eq!(p, 2_700_000);
        assert_eq!(tb, TimeBase::new(1, 90_000));
    }

    #[test]
    fn seek_target_foreign_stream_without_info_passes_pts_unscaled() {
        // Routed stream has no StreamInfo (defensive arm): keep the
        // pts and the command's time base rather than inventing one.
        let routes: Vec<(u32, ())> = vec![(3, ())];
        let streams = [stream_info(0, TimeBase::new(1, 8_000))];
        let c = cmd(9, 4_242, TimeBase::new(1, 1_000));
        let (s, p, tb) = resolve_seek_target(&routes, &streams, &c);
        assert_eq!(s, 3);
        assert_eq!(p, 4_242);
        assert_eq!(tb, TimeBase::new(1, 1_000));
    }

    #[test]
    fn seek_target_no_routes_falls_back_to_command() {
        // Degenerate: no routes at all (the caller filters these
        // sources out before spawning, but the helper must not panic).
        let routes: Vec<(u32, ())> = vec![];
        let streams: Vec<StreamInfo> = vec![];
        let c = cmd(0, 99, TimeBase::new(1, 48_000));
        let (s, p, tb) = resolve_seek_target(&routes, &streams, &c);
        assert_eq!(s, 0);
        assert_eq!(p, 99);
        assert_eq!(tb, TimeBase::new(1, 48_000));
    }

    #[test]
    fn channel_caps_default_matches_internal_constants() {
        // The default constructor must surface the same depth the
        // module previously hard-coded; existing callers (which pass
        // `None` for `caps`) get unchanged behaviour.
        let caps = ChannelCaps::default();
        assert_eq!(caps.packets, PACKET_CAP);
        assert_eq!(caps.frames, FRAME_CAP);
        let (p, f) = caps.resolved();
        assert_eq!(p, PACKET_CAP);
        assert_eq!(f, FRAME_CAP);
    }

    #[test]
    fn channel_caps_zero_promoted_to_one() {
        // `sync_channel(0)` is a rendezvous channel (every send blocks
        // until the consumer rendezvous-recv'd) and would serialise the
        // entire staged pipeline. `resolved()` clamps a request of 0 up
        // to 1 to give callers a meaningful "tightest legal" budget.
        let caps = ChannelCaps {
            packets: 0,
            frames: 0,
        };
        let (p, f) = caps.resolved();
        assert_eq!(p, 1, "packets=0 must be promoted to 1");
        assert_eq!(f, 1, "frames=0 must be promoted to 1");
    }

    #[test]
    fn channel_caps_arbitrary_values_round_trip() {
        // Above the clamp threshold the request is honoured verbatim
        // — operators picking `(64, 32)` for high-throughput offline
        // transcodes must see exactly that depth.
        let caps = ChannelCaps {
            packets: 64,
            frames: 32,
        };
        let (p, f) = caps.resolved();
        assert_eq!(p, 64);
        assert_eq!(f, 32);
    }

    #[test]
    fn queue_budget_zero_is_disabled() {
        // `0` means "no byte ceiling": `enabled()` is false, admit/release
        // are no-ops, and the in-flight total never moves off zero. This
        // is the default that preserves historical behaviour for callers
        // who never opt in.
        let b = QueueBudget::new(0);
        assert!(!b.enabled());
        b.admit(1_000_000);
        assert_eq!(b.in_flight.load(Ordering::SeqCst), 0);
        b.release(1_000_000);
        assert_eq!(b.in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn queue_budget_admit_release_balance() {
        // With a ceiling in force, admit and release move the in-flight
        // total symmetrically. After equal admit/release the total
        // returns to zero.
        let b = QueueBudget::new(4096);
        assert!(b.enabled());
        b.admit(100);
        b.admit(50);
        assert_eq!(b.in_flight.load(Ordering::SeqCst), 150);
        b.release(100);
        assert_eq!(b.in_flight.load(Ordering::SeqCst), 50);
        b.release(50);
        assert_eq!(b.in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn queue_budget_release_saturates_at_zero() {
        // A release larger than the in-flight total must clamp to zero
        // rather than wrap around `u64::MAX` — defensive against any
        // accounting skew between the demuxer's admit and the consumer's
        // release.
        let b = QueueBudget::new(4096);
        b.admit(10);
        b.release(1_000);
        assert_eq!(b.in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn queue_budget_wait_returns_when_below_ceiling() {
        // Below the ceiling, `wait_below_ceiling` returns immediately —
        // no parking. (We can't easily assert "blocks then unblocks"
        // without a second thread; the integration test covers the
        // back-pressure path end-to-end. Here we just confirm the
        // no-park fast path.)
        let abort = AbortState::new();
        let b = QueueBudget::new(4096);
        b.admit(100); // 100 < 4096
        b.wait_below_ceiling(&abort); // must not hang
    }

    #[test]
    fn queue_budget_wait_bails_on_abort() {
        // At/above the ceiling the demuxer would normally park; an abort
        // must release it so a stop/quit can't strand the demuxer.
        let abort = AbortState::new();
        let b = QueueBudget::new(100);
        b.admit(200); // 200 >= 100 — would park
        abort.request_abort();
        b.wait_below_ceiling(&abort); // must return promptly, not hang
    }

    struct FormatDiscoveringDecoder {
        codec_id: oxideav_core::CodecId,
        params: CodecParameters,
        pending: bool,
    }

    impl Decoder for FormatDiscoveringDecoder {
        fn codec_id(&self) -> &oxideav_core::CodecId {
            &self.codec_id
        }

        fn output_params(&self) -> Option<&CodecParameters> {
            Some(&self.params)
        }

        fn send_packet(&mut self, _packet: &Packet) -> Result<()> {
            self.params.sample_rate = Some(48_000);
            self.params.channels = Some(2);
            self.params.sample_format = Some(oxideav_core::SampleFormat::S16);
            self.pending = true;
            Ok(())
        }

        fn receive_frame(&mut self) -> Result<Frame> {
            if !self.pending {
                return Err(Error::NeedMore);
            }
            self.pending = false;
            Ok(Frame::Audio(oxideav_core::AudioFrame {
                samples: 1024,
                pts: Some(90_000),
                data: vec![vec![0; 1024 * 2 * 2]],
            }))
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn decode_stage_emits_authoritative_stream_update_before_first_frame() {
        let params = CodecParameters::audio(oxideav_core::CodecId::new("dynamic-audio"));
        let decoder: Box<dyn Decoder> = Box::new(FormatDiscoveringDecoder {
            codec_id: oxideav_core::CodecId::new("dynamic-audio"),
            params: params.clone(),
            pending: false,
        });
        let template = StreamInfo {
            index: 0,
            time_base: TimeBase::new(1, 90_000),
            duration: None,
            start_time: None,
            params,
        };
        let (packet_tx, packet_rx) = mpsc::sync_channel(2);
        let (frame_tx, frame_rx) = mpsc::sync_channel(4);
        let counters = Arc::new(PipelineCounters::default());
        let budget = QueueBudget::new(0);
        let abort = AbortState::new();

        packet_tx
            .send(Msg::Data(Packet::new(0, TimeBase::new(1, 90_000), vec![1])))
            .unwrap();
        packet_tx.send(Msg::Eof).unwrap();

        run_decode_stage(
            decoder,
            packet_rx,
            FrameDownstream::Channel(frame_tx),
            Some(template),
            abort,
            counters,
            budget,
        )
        .unwrap();

        let Msg::StreamUpdate(update) = frame_rx.recv().unwrap() else {
            panic!("expected stream update before decoded frame");
        };
        assert_eq!(update.params.sample_rate, Some(48_000));
        assert_eq!(update.params.channels, Some(2));
        assert_eq!(
            update.params.sample_format,
            Some(oxideav_core::SampleFormat::S16)
        );
        assert!(matches!(frame_rx.recv().unwrap(), Msg::Data(_)));
        assert!(matches!(frame_rx.recv().unwrap(), Msg::Eof));
    }

    struct CancellationBlockingDecoder {
        codec_id: oxideav_core::CodecId,
        pool: Arc<oxideav_core::arena::sync::ArenaPool>,
        _retained: oxideav_core::arena::sync::Arena,
        cancellation: Option<CancellationToken>,
    }

    impl Decoder for CancellationBlockingDecoder {
        fn codec_id(&self) -> &oxideav_core::CodecId {
            &self.codec_id
        }

        fn send_packet(&mut self, _packet: &Packet) -> Result<()> {
            let token = self
                .cancellation
                .as_ref()
                .expect("pipeline supplied cancellation token");
            self.pool.lease_wait_cancellable(token).map(|_| ())
        }

        fn receive_frame(&mut self) -> Result<Frame> {
            Err(Error::NeedMore)
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn set_cancellation_token(&mut self, token: CancellationToken) {
            self.cancellation = Some(token);
        }
    }

    #[test]
    fn decoder_arena_wait_unwinds_cleanly_on_pipeline_abort() {
        let pool = oxideav_core::arena::sync::ArenaPool::new(1, 64);
        let retained = pool.lease().expect("occupy only arena slot");
        let abort = AbortState::new();
        let mut decoder: Box<dyn Decoder> = Box::new(CancellationBlockingDecoder {
            codec_id: oxideav_core::CodecId::new("cancel-test"),
            pool: Arc::clone(&pool),
            _retained: retained,
            cancellation: None,
        });
        decoder.set_cancellation_token(abort.cancellation_token());

        let (packet_tx, packet_rx) = mpsc::sync_channel(1);
        let (frame_tx, _frame_rx) = mpsc::sync_channel(1);
        let counters = Arc::new(PipelineCounters::default());
        let budget = QueueBudget::new(0);
        let worker_abort = Arc::clone(&abort);
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = run_decode_stage(
                decoder,
                packet_rx,
                FrameDownstream::Channel(frame_tx),
                None,
                worker_abort,
                counters,
                budget,
            );
            done_tx.send(result).expect("report worker result");
        });

        packet_tx
            .send(Msg::Data(Packet::new(0, TimeBase::new(1, 1), vec![1])))
            .expect("send packet");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(30)).is_err(),
            "decoder should be blocked on its arena before abort"
        );

        abort.request_abort();
        let result = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("abort must wake the blocked decoder");
        assert!(
            result.is_ok(),
            "external abort should unwind cleanly: {result:?}"
        );
        worker.join().expect("decode worker");
    }
}
