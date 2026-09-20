//! DAG executor. Two modes share the same DAG-resolution + track-runtime
//! building code:
//!
//! - **Serial mode** (`threads == 1`): one packet pulled, one packet routed,
//!   one frame at a time. The original path — minimal, predictable, easy
//!   to reason about.
//! - **Pipelined mode** (`threads ≥ 2`, the default when
//!   `available_parallelism()` reports ≥ 2 cores): one thread per stage
//!   per track, bounded mpsc channels in between, mux on the caller
//!   thread so sinks don't need to be `Send`. See [`crate::pipeline`].
//!
//! Multi-output jobs run their outputs **concurrently** on the
//! pipelined path: outputs are chunked into document-order waves whose
//! width is clamped through [`ExecutionContext::effective_workers`]
//! (the same budget clamp codecs use for their internal fan-out), each
//! wave's outputs run on scoped threads, and the per-output codec
//! budget is the wave's even split of the job budget. `threads == 1`
//! keeps the historical strictly-sequential serial loop.

use std::collections::HashMap;
use std::path::PathBuf;

use oxideav_core::{
    CodecCapabilities, CodecId, CodecParameters, CodecRegistry, Decoder, Demuxer, Encoder, Error,
    ExecutionContext, FilterContext, FilterRegistry, Frame, FrameLease, FrameSource, MediaType,
    Packet, PacketSource, PixelFormat, PortParams, PortSpec, Rational, ReadSeek, Result,
    RuntimeContext, SampleFormat, SourceOutput, StreamFilter, StreamInfo, TimeBase,
};
use oxideav_pixfmt::{convert as pixfmt_convert, ConvertOptions};

use crate::dag::{codec_accepted_pixel_formats, Dag, DagNode, MuxTrack, ResolvedSelector};
use crate::failure::{attribute, FailureStage, RunFailure, StageFailure, StageResult};
use crate::schema::{is_reserved_sink, Job};
use crate::selection::{make_decoder_with_selection, make_encoder_with, CodecPreferences};
use crate::sinks::{open_file_write, FileSink, NullSink};
use crate::staged;

/// User-installable factory that builds a [`FrameSource`] for a
/// [`DagNode::Render3D`] node. Pipeline does not depend on
/// `oxideav-render`; the consumer (e.g. `oxideav-cli-convert`)
/// installs this callback at startup, bridging pipeline's name-and-JSON
/// description of a render request to a concrete `oxideav-render` +
/// `oxideav-mesh3d` setup.
///
/// Arguments: `(source_uri, backend_name, opts_json)` taken verbatim
/// from the [`DagNode::Render3D`] variant. The factory returns a
/// `Box<dyn FrameSource>` that the executor wraps in a
/// [`SourcePump::Frames`] and drives like any other frame-shape source.
///
/// Installed on the [`Executor`] via
/// [`Executor::with_render_source_factory`]. When unset, any
/// `Render3D` node in the DAG fails with an `Unsupported` error
/// pointing at the installation API — see the executor arm in
/// [`Executor::resolve_source_shapes`].
pub type RenderSourceFactory = Box<
    dyn Fn(&str, &str, &serde_json::Value) -> oxideav_core::Result<Box<dyn FrameSource>>
        + Send
        + Sync,
>;

/// One opened source, branched on the registered shape. Held in the
/// executor's per-URI map so the run loops can pump packets or frames
/// straight into the per-track stages without re-opening the source.
///
/// `Demuxer` is the historical shape (bytes → container probe → demuxer).
/// `Packets` skips the container layer (RTMP, RTSP, …). `Frames` skips
/// both demux + decode (synthetic generators).
pub(crate) enum SourcePump {
    Demuxer(Box<dyn Demuxer>),
    Packets(Box<dyn PacketSource>),
    Frames {
        source: Box<dyn FrameSource>,
        /// Synthesised single-stream descriptor — frame sources expose
        /// only `params()`, but the rest of the executor (selector
        /// matching, mux output stream layout) wants a `StreamInfo`.
        streams: Vec<StreamInfo>,
    },
}

impl SourcePump {
    pub(crate) fn streams(&self) -> &[StreamInfo] {
        match self {
            Self::Demuxer(d) => d.streams(),
            Self::Packets(p) => p.streams(),
            Self::Frames { streams, .. } => streams.as_slice(),
        }
    }
}

/// Build a one-element `StreamInfo` list for a [`FrameSource`]. The
/// selector logic in [`select_stream`] always picks index 0 for
/// frame-shape sources.
fn synth_stream_info(params: &CodecParameters) -> StreamInfo {
    // Time base: prefer the audio sample rate, else the video frame
    // rate's reciprocal, else microsecond ticks.
    let time_base = match params.media_type {
        MediaType::Audio => match params.sample_rate {
            Some(sr) if sr > 0 => TimeBase::new(1, sr as i64),
            _ => TimeBase::new(1, 1_000_000),
        },
        MediaType::Video => match params.frame_rate {
            Some(fr) if fr.num != 0 && fr.den != 0 => TimeBase::new(fr.den, fr.num),
            _ => TimeBase::new(1, 1_000_000),
        },
        _ => TimeBase::new(1, 1_000_000),
    };
    StreamInfo {
        index: 0,
        time_base,
        duration: None,
        start_time: Some(0),
        params: params.clone(),
    }
}

/// Metadata for one independently backpressured pipeline track.
///
/// A track sink is associated with one primary output stream. Multi-port filter
/// extras remain on the aggregate JobSink path for now; independent-track mode
/// is only selected when every pipeline track has exactly one sink-visible
/// stream.
#[derive(Clone, Debug)]
pub struct TrackSinkInfo {
    pub track_index: u32,
    pub stream: StreamInfo,
}

/// Terminal sink for one pipeline track.
///
/// Calls are made synchronously by the terminal worker for that track, so
/// blocking naturally backpressures only that track. Implementations that block
/// must honour the CancellationToken supplied by JobSink::open_track_sinks so
/// executor abort can wake the wait and tear the graph down cleanly.
pub trait TrackSink: Send {
    fn write_packet(&mut self, stream_index: u32, kind: MediaType, packet: Packet) -> Result<()>;

    fn write_frame_lease(
        &mut self,
        stream_index: u32,
        kind: MediaType,
        frame: FrameLease,
    ) -> Result<()>;

    fn stream_update(&mut self, _stream: &StreamInfo) -> Result<()> {
        Ok(())
    }

    fn barrier(&mut self, _barrier: crate::BarrierKind) -> Result<()> {
        Ok(())
    }
}

/// A user-installable output sink. Implementations receive either raw
/// packets (copy path) or decoded frames (transcode path without an
/// encoder node, e.g. live-play).
///
/// The trait itself does NOT require `Send` — the synchronous
/// [`Executor::run`] keeps the sink on the caller thread, and some
/// real-world sinks hold non-`Send` resources (e.g. a libloading
/// audio device). Sinks that want to participate in
/// [`Executor::spawn`] should additionally be `Send`; the spawn path
/// requires `Box<dyn JobSink + Send>` directly.
pub trait JobSink {
    /// Called once after all encoders are constructed and the output
    /// stream layout is known. Muxer-style sinks usually write the
    /// container header here.
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()>;

    /// Optionally split this output into independently backpressured track
    /// sinks. The staged runner calls this after start() and before media
    /// workers begin.
    ///
    /// Returning None preserves the aggregate JobSink callbacks below. Returning
    /// Some requires exactly one TrackSink per TrackSinkInfo, in the same order.
    /// Each returned sink is moved into that track's terminal worker; there is
    /// no final OxideAV output queue or central mux callback for those tracks.
    ///
    /// The serial executor does not split sinks and continues to use the
    /// aggregate callbacks.
    fn open_track_sinks(
        &mut self,
        _tracks: &[TrackSinkInfo],
        _cancellation: oxideav_core::CancellationToken,
    ) -> Result<Option<Vec<Box<dyn TrackSink + Send>>>> {
        Ok(None)
    }

    fn write_packet(&mut self, kind: MediaType, pkt: &Packet) -> Result<()>;
    fn write_frame(&mut self, kind: MediaType, frm: &Frame) -> Result<()>;

    /// Receive an owned decoded-frame lease without copying its backing
    /// storage. Player-style sinks should override this method and retain or
    /// forward the lease directly.
    ///
    /// The default adapter preserves source compatibility for existing sinks by
    /// materialising the lease only at this legacy `&Frame` boundary.
    fn write_frame_lease(&mut self, kind: MediaType, lease: FrameLease) -> Result<()> {
        let frame = lease.into_frame()?;
        self.write_frame(kind, &frame)
    }

    /// Notify the aggregate sink that a decoder has learned authoritative output
    /// parameters after start(). Independent-track delivery sends this callback
    /// to the corresponding TrackSink instead.
    fn stream_update(&mut self, _stream: &StreamInfo) -> Result<()> {
        Ok(())
    }

    /// Drain any remaining internal state and finalise the whole output.
    fn finish(&mut self) -> Result<()>;

    /// Flow-barrier hook for aggregate delivery. Independent-track delivery
    /// forwards barriers directly to the corresponding TrackSink.
    fn barrier(&mut self, _kind: crate::BarrierKind) -> Result<()> {
        Ok(())
    }

    /// Failed-output disposal hook. Called INSTEAD of Self::finish when the
    /// executor was configured with discard_failed_outputs and this output
    /// fails. Disposal errors never mask the original failure.
    fn abandon(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Job runner. Constructed with a validated `Job` and a unified
/// [`RuntimeContext`] that bundles every registry the framework needs
/// (codec / container / source / filter); dispatches to serial or
/// pipelined execution based on the effective thread budget.
pub struct Executor<'a> {
    job: &'a Job,
    ctx: &'a RuntimeContext,
    sink_overrides: HashMap<String, Box<dyn JobSink + Send>>,
    /// Explicit thread budget from the caller. `None` = resolve from
    /// `job.threads` or autodetect. `Some(0)` is treated as auto as well.
    explicit_threads: Option<usize>,
    /// Optional per-track channel-depth budget for the pipelined runner.
    /// `None` means "use [`staged::ChannelCaps::default()`]" (16
    /// packets, 8 frames). Ignored on the serial path. Set via
    /// [`Self::with_channel_caps`].
    channel_caps: Option<staged::ChannelCaps>,
    /// Aggregate byte ceiling on the demuxer→worker packet queues for
    /// the pipelined runner. `0` (the default) disables the byte
    /// ceiling, leaving only the count caps. Ignored on the serial
    /// path. Set via [`Self::with_max_queue_bytes`].
    max_queue_bytes: u64,
    /// When `true`, a failed output's sink receives
    /// [`JobSink::abandon`] instead of being silently dropped, so
    /// partial artifacts (e.g. the half-written file of a
    /// [`crate::FileSink`]) are removed. Default `false` — historical
    /// behaviour keeps whatever landed before the failure. Set via
    /// [`Self::with_discard_failed_outputs`].
    discard_failed_outputs: bool,
    /// Optional factory for [`DagNode::Render3D`] nodes. When `None`,
    /// any `Render3D` node in the DAG fails source-shape resolution
    /// with an `Unsupported` error pointing at where to install the
    /// callback. Set via [`Self::with_render_source_factory`].
    ///
    /// Phase C-3c of the render-pipeline integration: pipeline does
    /// not depend on `oxideav-render`. The consumer installs the
    /// closure to bridge `(source_uri, backend_name, opts_json)` to a
    /// concrete `Box<dyn FrameSource>`.
    render_source_factory: Option<RenderSourceFactory>,
    /// Codec implementation preferences applied when decode/encode stages are instantiated.
    /// Defaults to unconstrained selection.
    codec_preferences: CodecPreferences,
}

impl<'a> Executor<'a> {
    /// Construct an executor over `job` using the registries bundled in
    /// `ctx`. Callers configure `ctx.filters` (and any optional codec
    /// or container registrations) before constructing the Executor —
    /// the executor itself doesn't mutate the context.
    pub fn new(job: &'a Job, ctx: &'a RuntimeContext) -> Self {
        Self {
            job,
            ctx,
            sink_overrides: HashMap::new(),
            explicit_threads: None,
            channel_caps: None,
            max_queue_bytes: 0,
            discard_failed_outputs: false,
            render_source_factory: None,
            codec_preferences: CodecPreferences::default(),
        }
    }

    /// Replace the sink for a named output. Typically used to bind a
    /// live-playback sink to `@display`/`@out`.
    ///
    /// The sink is required to be `Send` so it can move onto the
    /// background mux thread used by [`Self::spawn`]. Sinks consumed
    /// only by the synchronous [`Self::run`] path can still satisfy
    /// this with a wrapper that owns its non-`Send` resources via
    /// `Send`-able indirection.
    pub fn with_sink_override(mut self, name: &str, sink: Box<dyn JobSink + Send>) -> Self {
        self.sink_overrides.insert(name.to_string(), sink);
        self
    }

    /// Override the thread budget. `0` means "auto" (use the value from the
    /// job's `threads` key, or fall back to `available_parallelism()`).
    /// `1` forces strictly serial execution; `≥ 2` requests pipelined.
    pub fn with_threads(mut self, n: usize) -> Self {
        self.explicit_threads = Some(n);
        self
    }

    /// Override codec implementation selection for every decode/encode stage.
    /// This allows a consumer to require hardware acceleration or explicitly
    /// opt out while keeping all implementations registered in one runtime.
    pub fn with_codec_preferences(mut self, preferences: CodecPreferences) -> Self {
        self.codec_preferences = preferences;
        self
    }

    /// Override the per-track channel-depth budget for the pipelined
    /// runner. See [`staged::ChannelCaps`] for the memory-bound model.
    ///
    /// Has no effect on the serial path — that path uses no channels.
    ///
    /// Typical uses:
    /// * `ChannelCaps { packets: 1, frames: 1 }` — tightest possible
    ///   bound for embedded playback (the queues degenerate to one
    ///   element each, so the source thread blocks until the consumer
    ///   has drained).
    /// * `ChannelCaps { packets: 64, frames: 32 }` — loosened for
    ///   high-throughput offline transcodes where bursty decoders /
    ///   filters can amortise their per-call overhead on a deeper
    ///   queue rather than blocking on the encoder.
    pub fn with_channel_caps(mut self, caps: staged::ChannelCaps) -> Self {
        self.channel_caps = Some(caps);
        self
    }

    /// Cap the aggregate bytes buffered in the demuxer→worker packet
    /// queues for the pipelined runner. `0` (the default) disables the
    /// ceiling, leaving only the element-count caps from
    /// [`Self::with_channel_caps`].
    ///
    /// [`with_channel_caps`](Self::with_channel_caps) bounds the queues
    /// by *element count* — at most `packets` packets per track. That is
    /// the right knob when packet sizes are uniform, but a single
    /// outsized packet (a tracker module delivered whole in one packet,
    /// a 4K intra keyframe, a JPEG-2000 codestream) can be megabytes on
    /// its own, so the count cap alone can let resident memory swing
    /// widely with content. `with_max_queue_bytes` adds an orthogonal
    /// byte ceiling: the demuxer parks before reading the next packet
    /// while the in-flight packet bytes are at or above `n`, and the
    /// consuming stage (copy or decode) frees the bytes the instant it
    /// pulls the packet off the channel.
    ///
    /// The two knobs compose — whichever binds first applies. A lone
    /// packet larger than `n` is still admitted (we cross the line by
    /// one packet rather than deadlock), so `n` is a soft target, not a
    /// hard never-exceed.
    ///
    /// Has no effect on the serial path — that path uses no channels.
    pub fn with_max_queue_bytes(mut self, n: u64) -> Self {
        self.max_queue_bytes = n;
        self
    }

    /// Opt in to failed-output disposal: when an output fails, its
    /// sink receives [`JobSink::abandon`] instead of being silently
    /// dropped mid-stream.
    ///
    /// For the built-in file sink that means the partially-written
    /// output file is DELETED — a failed transcode leaves no
    /// half-file behind for a downstream consumer to mistake for a
    /// finished one. Custom sinks opt in by overriding
    /// [`JobSink::abandon`] (the default is a no-op, so enabling this
    /// knob is always safe with observer/override sinks).
    ///
    /// Scope of "failed": the output's own run erred after its sink
    /// was resolved; or, on a multi-output job, the sink was resolved
    /// for a wave whose preparation failed before the wave started
    /// (its file was already created but the output never ran).
    /// Healthy wave-mates of a failing output are unaffected — they
    /// run to completion and finalise normally — and a clean
    /// [`ExecutorHandle::stop`] (no recorded error) still finalises
    /// via [`JobSink::finish`] rather than abandoning.
    ///
    /// Default `false`: historical behaviour, partial output is kept
    /// wherever the sink put it (useful for debugging a mid-stream
    /// failure).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use oxideav_core::RuntimeContext;
    /// use oxideav_pipeline::{Executor, Job};
    ///
    /// let ctx = RuntimeContext::new();
    /// let job = Job::from_json(r#"{"out.wav": {"audio": [{"from": "in.wav"}]}}"#)?;
    /// // A failed run deletes the partially-written out.wav instead of
    /// // leaving a truncated file that looks like a finished output.
    /// let stats = Executor::new(&job, &ctx)
    ///     .with_discard_failed_outputs(true)
    ///     .run()?;
    /// # let _ = stats;
    /// # Ok::<(), oxideav_core::Error>(())
    /// ```
    pub fn with_discard_failed_outputs(mut self, discard: bool) -> Self {
        self.discard_failed_outputs = discard;
        self
    }

    /// Install the render-source factory used to materialise
    /// [`DagNode::Render3D`] leaves into a [`FrameSource`]. Pipeline
    /// does not depend on `oxideav-render`; the consumer (e.g.
    /// `oxideav-cli-convert`) installs this callback at startup so the
    /// executor can call into it when the DAG walk reaches a
    /// `Render3D` node. See [`RenderSourceFactory`].
    ///
    /// When unset, any `Render3D` node fails source-shape resolution
    /// with an `Unsupported` error pointing at this very method.
    pub fn with_render_source_factory(mut self, f: RenderSourceFactory) -> Self {
        self.render_source_factory = Some(f);
        self
    }

    /// Validate, resolve, and run the job.
    ///
    /// With `threads == 1` (or a single output on any budget) outputs
    /// process strictly in their document order, one at a time. With
    /// `threads ≥ 2` and several outputs, outputs run **concurrently**
    /// in document-order waves — see [`Self::run_outputs_parallel`]
    /// for the wave/budget model and the error-precedence contract.
    ///
    /// On failure the ORIGINAL first error is returned unchanged
    /// (first-error-wins, pinned by `tests/error_propagation.rs`). Use
    /// [`Self::run_reporting`] to additionally learn *where* the error
    /// fired — both surfaces observe the exact same root cause.
    pub fn run(self) -> Result<ExecutorStats> {
        self.run_reporting().map_err(Error::from)
    }

    /// Like [`Self::run`], but a failure returns a [`RunFailure`]
    /// carrying the attribution alongside the original error: the
    /// owning output key, the [`FailureStage`], the track index when
    /// the failing stage belongs to one, and the failing output's
    /// partial [`ExecutorStats`] ("how far did it get").
    ///
    /// `RunFailure::error` holds the exact error [`Self::run`] would
    /// have returned (never a re-stringified copy), so error-kind
    /// matching behaves identically on both surfaces.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use oxideav_core::RuntimeContext;
    /// use oxideav_pipeline::{Executor, FailureStage, Job};
    ///
    /// let ctx = RuntimeContext::new();
    /// let job = Job::from_json(r#"{"out.wav": {"audio": [{"from": "in.wav"}]}}"#)?;
    /// match Executor::new(&job, &ctx).run_reporting() {
    ///     Ok(stats) => println!("done: {} packets", stats.packets_read),
    ///     Err(f) if f.stage == FailureStage::Prepare => {
    ///         log::warn!("setup failed before any data flowed: {f}");
    ///     }
    ///     Err(f) => {
    ///         log::warn!(
    ///             "{} failed mid-stream after {} frames: {}",
    ///             f.output.as_deref().unwrap_or("job"),
    ///             f.stats.frames_written,
    ///             f.error,
    ///         );
    ///     }
    /// }
    /// # Ok::<(), oxideav_core::Error>(())
    /// ```
    pub fn run_reporting(mut self) -> std::result::Result<ExecutorStats, RunFailure> {
        self.job.validate().map_err(RunFailure::job_level)?;
        let dag = self.job.to_dag().map_err(RunFailure::job_level)?;
        let threads = self.resolve_threads();
        let names: Vec<String> = dag.roots.keys().cloned().collect();
        if threads >= 2 && names.len() >= 2 {
            return self.run_outputs_parallel(&dag, &names, threads);
        }
        let mut stats = ExecutorStats::default();
        for name in names {
            let out_stats = if threads >= 2 {
                self.run_output_pipelined(&dag, &name, threads)
            } else {
                self.run_output(&dag, &name)
            }
            .map_err(|f| RunFailure::for_output(&name, f))?;
            stats.merge(&out_stats);
        }
        Ok(stats)
    }

    /// Run a multi-output job with the outputs executing concurrently.
    ///
    /// **Wave model.** Outputs are processed in document-order chunks
    /// ("waves"). The wave width is
    /// `ExecutionContext::with_threads(threads).effective_workers(n_outputs)`
    /// — the exact clamp codecs use for internal fan-out, so a
    /// 64-output job on a 4-thread budget runs at most 4 outputs at a
    /// time instead of spawning 64 pipelines at once. Within a wave,
    /// every output gets an even split of the job budget
    /// (`threads / wave_width`, floored, min 1) as its codec-internal
    /// [`ExecutionContext`] budget; the structural stage-per-thread
    /// pipeline is used for every output either way. The next wave
    /// starts only after every output of the current wave finished.
    ///
    /// **Preparation stays sequential.** Source opening, codec
    /// instantiation, and sink resolution for a wave happen on the
    /// caller thread in document order before the wave spawns, so
    /// setup errors keep their document-order precedence and never
    /// interleave.
    ///
    /// **Error precedence.** If any output of a wave fails, `run`
    /// returns the error of the *earliest failing output in document
    /// order* (deterministic regardless of thread timing), and later
    /// waves never start. Unlike the sequential path, the failing
    /// output's wave-mates run to completion before the error
    /// surfaces — each output is an independent pipeline with its own
    /// abort cascade, and a healthy sibling's data is delivered rather
    /// than discarded. Outputs in waves after the failure never start,
    /// matching the sequential contract.
    fn run_outputs_parallel(
        &mut self,
        dag: &Dag,
        names: &[String],
        threads: usize,
    ) -> std::result::Result<ExecutorStats, RunFailure> {
        let budget = ExecutionContext::with_threads(threads);
        let wave_width = budget.effective_workers(names.len());
        let per_output = (threads / wave_width).max(1);
        let mut stats = ExecutorStats::default();
        for wave in names.chunks(wave_width) {
            // Prepare sequentially (document order) on the caller
            // thread; run the prepared outputs concurrently.
            let mut preps: Vec<(String, PreparedRun)> = Vec::with_capacity(wave.len());
            for name in wave {
                match self.prepare_pipelined_run(dag, name, per_output) {
                    Ok(p) => preps.push((name.clone(), p)),
                    Err(e) => {
                        // Wave-mates prepared before this failure will
                        // never run — their sinks (and any artifacts
                        // sink resolution created, e.g. a FileSink's
                        // output file) are dead weight. Give them the
                        // failed-output disposal treatment when opted
                        // in; disposal errors never mask the prep
                        // error.
                        if self.discard_failed_outputs {
                            for (_, mut p) in preps {
                                let _ = p.sink.abandon();
                            }
                        }
                        return Err(RunFailure::for_output(
                            name,
                            StageFailure::new(FailureStage::Prepare, None, e),
                        ));
                    }
                }
            }
            let wave_names: Vec<String> = preps.iter().map(|(n, _)| n.clone()).collect();
            let results: Vec<StageResult<ExecutorStats>> = std::thread::scope(|s| {
                let handles: Vec<_> = preps
                    .into_iter()
                    .map(|(name, prep)| {
                        std::thread::Builder::new()
                            .name(format!("pipeline-out-{name}"))
                            .spawn_scoped(s, move || {
                                staged::run_pipelined(
                                    prep.pipelines,
                                    prep.sources_by_uri,
                                    prep.sink,
                                    prep.out_streams,
                                    prep.channel_caps,
                                    prep.max_queue_bytes,
                                    prep.discard_on_failure,
                                )
                            })
                            .expect("oxideav-pipeline: failed to spawn output thread")
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            // The output thread runs the mux loop +
                            // sink; a panic that escapes the per-stage
                            // catch surfaces there, so attribute it to
                            // the sink side rather than inventing a
                            // separate variant for a case that should
                            // never happen.
                            Err(StageFailure::new(
                                FailureStage::Sink,
                                None,
                                Error::other("pipeline: output worker thread panicked"),
                            ))
                        })
                    })
                    .collect()
            });
            // Document-order iteration ⇒ the first error returned is
            // the earliest failing output in document order.
            for (name, r) in wave_names.iter().zip(results) {
                stats.merge(&r.map_err(|f| RunFailure::for_output(name, f))?);
            }
        }
        Ok(stats)
    }

    /// Spawn the executor on a background thread and return a handle
    /// for live control (seek, abort, progress). Unlike [`Self::run`],
    /// this only supports a single output (the typical playback case
    /// — one `@display` / `@out`). For multi-output transcodes,
    /// continue to use [`Self::run`].
    ///
    /// All the demuxer-open / decoder-instantiate / sink-resolve work
    /// happens synchronously up front — the returned handle is live by
    /// the time `spawn` returns. The mux + worker threads run in the
    /// background until either the demuxer hits EOF, [`ExecutorHandle::stop`]
    /// is called, or any worker reports an error.
    pub fn spawn(mut self) -> Result<ExecutorHandle> {
        self.job.validate()?;
        let dag = self.job.to_dag()?;
        let threads = self.resolve_threads().max(2);
        let names: Vec<String> = dag.roots.keys().cloned().collect();
        let name = match names.as_slice() {
            [n] => n.clone(),
            [] => return Err(Error::invalid("job: no outputs to spawn")),
            _ => {
                return Err(Error::invalid(
                    "job: spawn() supports a single output today; use run() for multi-output jobs",
                ));
            }
        };
        let prep = self.prepare_pipelined_run(&dag, &name, threads)?;
        Ok(ExecutorHandle::start(prep))
    }

    /// Resolve the effective thread budget. Priority: explicit
    /// `with_threads(n > 0)` > `job.threads` > host autodetect.
    ///
    /// The autodetect goes through [`ExecutionContext::auto`] rather
    /// than querying the host directly — `ExecutionContext` is the
    /// single threading authority in the framework, and host-derived
    /// budgets are the caller-side decision it encapsulates. The
    /// executor is that caller; everything downstream (codec fan-out,
    /// the multi-output wave width) clamps through
    /// [`ExecutionContext::effective_workers`] against the budget
    /// resolved here.
    fn resolve_threads(&self) -> usize {
        if let Some(n) = self.explicit_threads {
            if n > 0 {
                return n;
            }
        }
        if let Some(n) = self.job.threads {
            if n > 0 {
                return n;
            }
        }
        ExecutionContext::auto().threads
    }

    fn run_output(&mut self, dag: &Dag, name: &str) -> StageResult<ExecutorStats> {
        // Everything up to (and including) sink resolution is
        // preparation — no stream data has flowed — so its errors
        // attribute to `FailureStage::Prepare`.
        let prep = attribute(FailureStage::Prepare, None);
        let mut dag = dag.clone();
        // Probe every Demuxer-shape leaf and rewrite to the typed
        // variant (PacketSource / FrameSource) when the registry hands
        // back a non-bytes opener. Returns the opened sources keyed by
        // URI so we don't re-open below.
        let mut sources_by_uri = self.resolve_source_shapes(&mut dag).map_err(&prep)?;
        let dag = &dag;
        let root_id = dag.roots[name];
        let tracks: Vec<MuxTrack> = match dag.node(root_id) {
            DagNode::Mux { tracks, .. } => tracks.clone(),
            other => {
                return Err(prep(Error::invalid(format!(
                    "job: output {name}: expected Mux root, got {other:?}"
                ))));
            }
        };

        // Walk each track's upstream chain to find the source leaf + the
        // stack of stages (select/decode/filter/encode) to apply.
        let mut pipelines: Vec<TrackRuntime> = Vec::new();
        for t in &tracks {
            pipelines.push(self.build_track_runtime(dag, t).map_err(&prep)?);
        }

        // Expand `all:` tracks (kind == Unknown, selector == any) into
        // one TrackRuntime per source stream now that the source can
        // be queried for its stream list.
        pipelines = expand_all_tracks_pump(pipelines, &sources_by_uri);

        // Resolve each pipeline's stream index now that we can inspect
        // the source's stream list. For frame sources `select_stream`
        // always returns the synthetic index 0.
        for pl in &mut pipelines {
            let pump = sources_by_uri.get(&pl.source_uri).ok_or_else(|| {
                prep(Error::invalid(format!(
                    "job: track references source {:?} but no source was opened for that URI \
                     (known URIs: {:?}) — this is an internal pipeline-resolver bug",
                    pl.source_uri,
                    sources_by_uri.keys().collect::<Vec<_>>(),
                )))
            })?;
            pl.source_stream = select_stream(pump.streams(), &pl.selector).map_err(&prep)?;
            let info = pump
                .streams()
                .iter()
                .find(|s| s.index == pl.source_stream)
                .ok_or_else(|| prep(Error::invalid("selected stream not in source")))?;
            pl.input_params = info.params.clone();
            pl.input_time_base = info.time_base;
            pl.input_start_time = info.start_time;
            pl.input_duration = info.duration;
        }

        // Drop pumps no track of THIS output uses — the DAG probe in
        // `resolve_source_shapes` opens every source leaf in the
        // document, but this run is per-output. Without the retain the
        // pump loop below would drain (and stat-count) sources that
        // belong to the job's OTHER outputs: on a multi-output job
        // every output re-read every source, inflating `packets_read`
        // and doing dead work proportional to the output count. The
        // pipelined path has always dropped them in
        // `prepare_pipelined_run`; this keeps serial/pipelined stats
        // parity on multi-output jobs.
        let used: std::collections::HashSet<&String> =
            pipelines.iter().map(|pl| &pl.source_uri).collect();
        sources_by_uri.retain(|uri, _| used.contains(uri));

        // Auto-insert pixel-format conversion stages now that we know
        // the source stream's pixel format. Runs after the source is
        // open and before codec instantiation.
        for pl in &mut pipelines {
            pl.apply_pixel_format_auto_insert(&self.ctx.codecs);
        }

        // Instantiate decoders / filters / encoders for each track. The
        // serial path tells codecs to stay single-threaded; the pipelined
        // path passes its own thread budget below.
        let ctx = ExecutionContext::serial();
        for pl in &mut pipelines {
            pl.instantiate(
                &self.ctx.codecs,
                &self.codec_preferences,
                &ctx,
                &self.ctx.filters,
            )
            .map_err(&prep)?;
        }

        // Build the per-track output stream infos + open (or replace) the sink.
        // Multi-port filters add extra streams after their primary — the sink
        // sees all of them in `start()` before any frame flows.
        let out_streams = build_output_streams(&mut pipelines);

        let mut sink = self.open_sink(name, &out_streams).map_err(&prep)?;
        let result = pump_serial_output(
            &mut pipelines,
            &mut sources_by_uri,
            sink.as_mut(),
            &out_streams,
        );
        if result.is_err() && self.discard_failed_outputs {
            // Failed-output disposal (opt-in): the sink cleans up its
            // partial artifacts instead of leaving a half-written
            // output behind. Disposal errors never mask the run error.
            let _ = sink.abandon();
        }
        result
    }
}

/// Serial pump for one output: round-robin every source to EOF, feeding
/// packets / frames through the per-track runtimes into the sink, then
/// drain + finalise. Split out of `Executor::run_output` so the caller
/// retains sink ownership across a failure and can run the
/// failed-output disposal hook ([`JobSink::abandon`]) on it.
fn pump_serial_output(
    pipelines: &mut [TrackRuntime],
    sources_by_uri: &mut HashMap<String, SourcePump>,
    sink: &mut dyn JobSink,
    out_streams: &[StreamInfo],
) -> StageResult<ExecutorStats> {
    sink.start(out_streams)
        .map_err(attribute(FailureStage::Sink, None))?;
    let mut stats = ExecutorStats::default();
    // On failure, attach the partial counters to the attribution so
    // callers see "how far it got" (`RunFailure::stats`).
    match pump_serial_streams(pipelines, sources_by_uri, sink, &mut stats) {
        Ok(()) => Ok(stats),
        Err(mut failure) => {
            failure.stats = stats;
            Err(failure)
        }
    }
}

/// The serial pump proper, accumulating into `stats` owned by the
/// caller so a mid-stream failure still reports the partial counters.
fn pump_serial_streams(
    pipelines: &mut [TrackRuntime],
    sources_by_uri: &mut HashMap<String, SourcePump>,
    sink: &mut dyn JobSink,
    stats: &mut ExecutorStats,
) -> StageResult<()> {
    // Main pump. Read packets / frames from every source in
    // round-robin until all are EOF. Routes vary by source shape:
    //   - Demuxer / Packets → next_packet()  → feed_packet()
    //   - Frames            → next_frame()   → feed_frame()
    let mut eof: HashMap<String, bool> =
        sources_by_uri.keys().map(|k| (k.clone(), false)).collect();
    let uris: Vec<String> = sources_by_uri.keys().cloned().collect();
    while eof.values().any(|e| !e) {
        for uri in &uris {
            if eof[uri] {
                continue;
            }
            let pump = sources_by_uri.get_mut(uri).unwrap();
            match pump {
                SourcePump::Demuxer(dmx) => match dmx.next_packet() {
                    Ok(pkt) => {
                        stats.packets_read += 1;
                        for (track_idx, pl) in pipelines.iter_mut().enumerate() {
                            if pl.source_uri != *uri {
                                continue;
                            }
                            if pkt.stream_index != pl.source_stream {
                                continue;
                            }
                            pl.feed_packet(&pkt, track_idx as u32, &mut *sink, stats)?;
                        }
                    }
                    Err(Error::Eof) => {
                        eof.insert(uri.clone(), true);
                        continue;
                    }
                    Err(e) => return Err(attribute(FailureStage::Source, None)(e)),
                },
                SourcePump::Packets(pkts) => match pkts.next_packet() {
                    Ok(pkt) => {
                        stats.packets_read += 1;
                        for (track_idx, pl) in pipelines.iter_mut().enumerate() {
                            if pl.source_uri != *uri {
                                continue;
                            }
                            if pkt.stream_index != pl.source_stream {
                                continue;
                            }
                            pl.feed_packet(&pkt, track_idx as u32, &mut *sink, stats)?;
                        }
                    }
                    Err(Error::Eof) => {
                        eof.insert(uri.clone(), true);
                        continue;
                    }
                    Err(e) => return Err(attribute(FailureStage::Source, None)(e)),
                },
                SourcePump::Frames { source, .. } => match source.next_frame() {
                    Ok(frame) => {
                        // Frame sources have a single synthetic
                        // stream — every track on this URI consumes
                        // the same frame. We clone for each track
                        // beyond the first so ownership works out.
                        stats.frames_decoded += 1;
                        let mut consumers: Vec<usize> = Vec::new();
                        for (track_idx, pl) in pipelines.iter().enumerate() {
                            if pl.source_uri == *uri {
                                consumers.push(track_idx);
                            }
                        }
                        for &track_idx in &consumers {
                            pipelines[track_idx].feed_frame(
                                frame.clone(),
                                track_idx as u32,
                                &mut *sink,
                                stats,
                            )?;
                        }
                    }
                    Err(Error::Eof) => {
                        eof.insert(uri.clone(), true);
                        continue;
                    }
                    Err(e) => return Err(attribute(FailureStage::Source, None)(e)),
                },
            }
        }
    }
    // EOF — drain each pipeline.
    for (track_idx, pl) in pipelines.iter_mut().enumerate() {
        pl.drain(track_idx as u32, &mut *sink, stats)?;
    }
    sink.finish()
        .map_err(attribute(FailureStage::SinkFinish, None))?;
    Ok(())
}

impl<'a> Executor<'a> {
    pub(crate) fn build_track_runtime(&self, dag: &Dag, track: &MuxTrack) -> Result<TrackRuntime> {
        // Walk upstream chain, accumulating stages in reverse (top-down).
        // The chain ends at a Demuxer / PacketSource / FrameSource leaf.
        // The pixel-format auto-insert pass runs later, once the source
        // is open and its `CodecParameters.pixel_format` is known —
        // see `TrackRuntime::apply_pixel_format_auto_insert`.
        let mut stages: Vec<StageSpec> = Vec::new();
        let mut leaf_is_frames = false;
        let mut cur = track.upstream;
        let (source_uri, selector) = loop {
            match dag.node(cur) {
                DagNode::Demuxer { source } => {
                    break (source.clone(), ResolvedSelector::any());
                }
                DagNode::PacketSource { source } => {
                    // PacketSource is a packet-producing leaf — same
                    // shape as Demuxer for the rest of the pipeline.
                    break (source.clone(), ResolvedSelector::any());
                }
                DagNode::FrameSource { source } => {
                    // FrameSource is a frame-producing leaf — no decode
                    // stage in the upstream chain. The runtime detects
                    // this via the `Frames` SourcePump variant and
                    // routes frames straight into the filter chain.
                    leaf_is_frames = true;
                    break (source.clone(), ResolvedSelector::any());
                }
                DagNode::Render3D {
                    source, backend, ..
                } => {
                    // Phase C-3c: `resolve_source_shapes` runs ahead of
                    // `build_track_runtime` and either rewrites
                    // `Render3D` nodes to `FrameSource { source }` (when
                    // a `render_source_factory` is installed) or fails
                    // up front with an `Unsupported` error. Reaching
                    // this arm means the resolver was skipped — an
                    // internal pipeline-resolver bug, not a user error.
                    return Err(Error::other(format!(
                        "internal: Render3D node for source {source:?} (backend \
                         {backend:?}) reached build_track_runtime — \
                         resolve_source_shapes should have rewritten it first"
                    )));
                }
                DagNode::Select { upstream, selector } => match dag.node(*upstream) {
                    DagNode::Demuxer { source } | DagNode::PacketSource { source } => {
                        break (source.clone(), selector.clone());
                    }
                    DagNode::FrameSource { source } => {
                        // A `Select` over a frame source is degenerate —
                        // there's exactly one (synthetic) stream — but
                        // honour the kind constraint so a job that asks
                        // for `audio:` on a video frame source still
                        // errors loudly downstream rather than silently
                        // muxing a video frame as audio.
                        leaf_is_frames = true;
                        break (source.clone(), selector.clone());
                    }
                    _ => {
                        return Err(Error::other(
                            "job: nested Select above non-source-leaf is not yet supported",
                        ));
                    }
                },
                DagNode::Decode { upstream } => {
                    stages.push(StageSpec::Decode);
                    cur = *upstream;
                }
                DagNode::Filter {
                    upstream,
                    name,
                    params,
                } => {
                    stages.push(StageSpec::Filter {
                        name: name.clone(),
                        params: params.clone(),
                    });
                    cur = *upstream;
                }
                DagNode::PixConvert { upstream, target } => {
                    stages.push(StageSpec::Convert { target: *target });
                    cur = *upstream;
                }
                DagNode::Encode {
                    upstream,
                    codec,
                    params,
                } => {
                    stages.push(StageSpec::Encode {
                        codec: codec.clone(),
                        params: params.clone(),
                    });
                    cur = *upstream;
                }
                DagNode::Mux { .. } => {
                    return Err(Error::other(
                        "job: walked into a Mux node while building track runtime",
                    ));
                }
            }
        };
        stages.reverse();
        // FrameSource leaves emit decoded frames directly — drop any
        // `Decode` stage that the resolver inserted between the source
        // and the first filter / encode. Without this, the runtime
        // would try to feed frames into a decoder that expects packets.
        if leaf_is_frames {
            stages.retain(|s| !matches!(s, StageSpec::Decode));
        }
        Ok(TrackRuntime::new(
            source_uri, selector, track.kind, track.copy, stages,
        ))
    }

    /// Open a source URI into a [`SourcePump`]. Branches on the
    /// [`SourceOutput`] returned by the source registry — bytes-shape
    /// sources go through the historical container-probe + demuxer-open
    /// dance; packet-shape and frame-shape sources are wrapped directly.
    pub(crate) fn open_source(&self, uri: &str) -> Result<SourcePump> {
        match self.ctx.sources.open(uri)? {
            SourceOutput::Bytes(bytes) => {
                // The trait alias `BytesSource: Read + Seek + Send` and
                // `ReadSeek: Read + Seek` differ only in the explicit
                // `Send` bound; container open wants `Box<dyn ReadSeek>`.
                // Re-box through a shim so the lifetimes line up without
                // adding a `Send` bound to the existing demuxer trait.
                let mut file: Box<dyn ReadSeek> = Box::new(bytes);
                let ext = ext_from_uri(uri);
                let format = self
                    .ctx
                    .containers
                    .probe_input(&mut *file, ext.as_deref())?;
                let dmx = self
                    .ctx
                    .containers
                    .open_demuxer(&format, file, &self.ctx.codecs)?;
                Ok(SourcePump::Demuxer(dmx))
            }
            SourceOutput::Packets(p) => Ok(SourcePump::Packets(p)),
            SourceOutput::Frames(f) => {
                let streams = vec![synth_stream_info(f.params())];
                Ok(SourcePump::Frames { source: f, streams })
            }
            // Multi-title sources don't fit the single-stream
            // pipeline shape — the CLI fans them out into N pipelines
            // via `oxideav remux %s.<ext>`. A direct pipeline open
            // here is a usage error.
            SourceOutput::MultiTitle(_) => Err(Error::unsupported(format!(
                "{uri}: this URI opens a multi-title source; use \
                 `oxideav remux <uri> <pattern-with-%s>` to fan out \
                 one output file per title"
            ))),
            // Any future source variant added behind `#[non_exhaustive]`
            // lands here until the pipeline executor is extended.
            _ => Err(Error::unsupported(format!(
                "{uri}: source kind not yet supported by the pipeline executor"
            ))),
        }
    }

    /// Open every unique source URI referenced by a `Demuxer` node in
    /// `dag`, then rewrite the node to `PacketSource` / `FrameSource`
    /// when the registry hands back a non-bytes shape. Returns the
    /// opened sources keyed by URI so the caller can move them into the
    /// run loop without re-opening.
    ///
    /// Bytes-shape sources keep their `Demuxer` node — the container
    /// probe + demuxer-open already happened during this probe call,
    /// and the resulting `Box<dyn Demuxer>` is returned in the map.
    pub(crate) fn resolve_source_shapes(
        &self,
        dag: &mut Dag,
    ) -> Result<HashMap<String, SourcePump>> {
        // Collect unique source URIs first to avoid double-opening when
        // the same source appears in multiple places (alias inlining,
        // multi-track outputs all reading the same input).
        let mut uris: Vec<(usize, String)> = Vec::new();
        // Render3D nodes carry an extra backend + opts payload that
        // doesn't live on `Demuxer` — collect them separately so the
        // factory call sees the full argument triple verbatim.
        let mut render_nodes: Vec<(usize, String, String, serde_json::Value)> = Vec::new();
        for (idx, node) in dag.nodes().iter().enumerate() {
            match node {
                DagNode::Demuxer { source } => uris.push((idx, source.clone())),
                DagNode::Render3D {
                    source,
                    backend,
                    opts,
                } => render_nodes.push((idx, source.clone(), backend.clone(), opts.clone())),
                _ => {}
            }
        }
        let mut opened: HashMap<String, SourcePump> = HashMap::new();
        for (node_idx, uri) in uris {
            if !opened.contains_key(&uri) {
                let pump = self.open_source(&uri)?;
                opened.insert(uri.clone(), pump);
            }
            // Rewrite the node based on what the source actually is.
            match opened.get(&uri).unwrap() {
                SourcePump::Demuxer(_) => {
                    // Stay as Demuxer — historical path.
                }
                SourcePump::Packets(_) => {
                    dag.nodes_mut()[node_idx] = DagNode::PacketSource {
                        source: uri.clone(),
                    };
                }
                SourcePump::Frames { .. } => {
                    dag.nodes_mut()[node_idx] = DagNode::FrameSource {
                        source: uri.clone(),
                    };
                }
            }
        }
        // Phase C-3c: materialise Render3D leaves through the installed
        // `render_source_factory`. Each (source, backend, opts) triple
        // becomes a `SourcePump::Frames` keyed by source URI; the DAG
        // node is rewritten in place to `FrameSource { source }` so the
        // rest of the executor treats it as any other frame leaf.
        //
        // Multiple `Render3D` nodes sharing the same `source` URI are a
        // user error — the factory would be called twice and we'd have
        // to pick which pump wins. Detect and refuse explicitly.
        for (node_idx, source, backend, opts) in render_nodes {
            let factory = self.render_source_factory.as_ref().ok_or_else(|| {
                Error::unsupported(format!(
                    "Render3D node for source {source:?} (backend {backend:?}) requires a \
                     render_source_factory on the Executor — install via \
                     Executor::with_render_source_factory(...)"
                ))
            })?;
            if opened.contains_key(&source) {
                return Err(Error::invalid(format!(
                    "Render3D node for source {source:?} collides with an already-opened \
                     source URI; each source URI must be unique in the DAG"
                )));
            }
            let frames = factory(&source, &backend, &opts)?;
            let streams = vec![synth_stream_info(frames.params())];
            opened.insert(
                source.clone(),
                SourcePump::Frames {
                    source: frames,
                    streams,
                },
            );
            dag.nodes_mut()[node_idx] = DagNode::FrameSource { source };
        }
        Ok(opened)
    }

    /// Pipelined counterpart to [`Self::run_output`]. Builds the same
    /// per-track runtimes + sources + sink, then hands them off to
    /// [`crate::staged::run_pipelined`] which spawns a stage-per-thread
    /// worker graph.
    ///
    /// All three source shapes run pipelined: bytes-shape sources get
    /// a demuxer thread, packet-shape sources get a packet-pump thread
    /// (no container layer), and frame-shape sources get a frame-pump
    /// thread feeding the per-track frame-stage chains directly (no
    /// demux, no decode). Historically only bytes-shape was supported
    /// and any typed source fell back to the serial [`Self::run_output`].
    fn run_output_pipelined(
        &mut self,
        dag: &Dag,
        name: &str,
        threads: usize,
    ) -> StageResult<ExecutorStats> {
        let prep = self
            .prepare_pipelined_run(dag, name, threads)
            .map_err(attribute(FailureStage::Prepare, None))?;
        staged::run_pipelined(
            prep.pipelines,
            prep.sources_by_uri,
            prep.sink,
            prep.out_streams,
            prep.channel_caps,
            prep.max_queue_bytes,
            prep.discard_on_failure,
        )
    }

    /// Shared prep used by both [`Self::run_output_pipelined`] and
    /// [`Self::spawn`]. Resolves source shapes, builds + instantiates
    /// the per-track runtimes, and resolves the sink. The returned
    /// struct is fully owned (no `'a`-borrowed members) so it can be
    /// moved onto a background thread.
    fn prepare_pipelined_run(
        &mut self,
        dag: &Dag,
        name: &str,
        threads: usize,
    ) -> Result<PreparedRun> {
        // Probe every source leaf and rewrite `Demuxer` nodes to their
        // typed variants — the same pass the serial path runs — so
        // `build_track_runtime` sees the authoritative source shape
        // (FrameSource leaves drop their Decode stages).
        let mut dag = dag.clone();
        let mut sources_by_uri = self.resolve_source_shapes(&mut dag)?;
        let dag = &dag;
        let root_id = dag.roots[name];
        let tracks: Vec<MuxTrack> = match dag.node(root_id) {
            DagNode::Mux { tracks, .. } => tracks.clone(),
            other => {
                return Err(Error::invalid(format!(
                    "job: output {name}: expected Mux root, got {other:?}"
                )));
            }
        };

        let mut pipelines: Vec<TrackRuntime> = Vec::new();
        for t in &tracks {
            pipelines.push(self.build_track_runtime(dag, t)?);
        }
        // Expand `all:` tracks (kind == Unknown, selector == any) into
        // one TrackRuntime per source stream. Done after sources open
        // so the stream kinds are authoritative.
        pipelines = expand_all_tracks_pump(pipelines, &sources_by_uri);
        for pl in &mut pipelines {
            let pump = sources_by_uri.get(&pl.source_uri).ok_or_else(|| {
                Error::invalid(format!(
                    "job: track references source {:?} but no source was opened for that \
                     URI — this is an internal pipeline-resolver bug",
                    pl.source_uri
                ))
            })?;
            pl.source_stream = select_stream(pump.streams(), &pl.selector)?;
            let info = pump
                .streams()
                .iter()
                .find(|s| s.index == pl.source_stream)
                .ok_or_else(|| Error::invalid("selected stream not in source"))?;
            pl.input_params = info.params.clone();
            pl.input_time_base = info.time_base;
            pl.input_start_time = info.start_time;
            pl.input_duration = info.duration;
        }
        // Auto-insert pixel-format conversion stages now that we know
        // the source stream's pixel format.
        for pl in &mut pipelines {
            pl.apply_pixel_format_auto_insert(&self.ctx.codecs);
        }
        let ctx = ExecutionContext::with_threads(threads);
        for pl in &mut pipelines {
            pl.instantiate(
                &self.ctx.codecs,
                &self.codec_preferences,
                &ctx,
                &self.ctx.filters,
            )?;
        }
        let out_streams = build_output_streams(&mut pipelines);
        let sink = self.open_sink(name, &out_streams)?;
        // Drop pumps no track of THIS output uses (the DAG probe opens
        // every source leaf in the document, but prep is per-output).
        // The staged runner would skip route-less pumps anyway;
        // dropping here releases the handles promptly.
        let used: std::collections::HashSet<&String> =
            pipelines.iter().map(|pl| &pl.source_uri).collect();
        sources_by_uri.retain(|uri, _| used.contains(uri));
        Ok(PreparedRun {
            output_name: name.to_string(),
            pipelines,
            sources_by_uri,
            sink,
            out_streams,
            channel_caps: self.channel_caps,
            max_queue_bytes: self.max_queue_bytes,
            discard_on_failure: self.discard_failed_outputs,
        })
    }

    pub(crate) fn open_sink(
        &mut self,
        name: &str,
        out_streams: &[StreamInfo],
    ) -> Result<Box<dyn JobSink + Send>> {
        if let Some(s) = self.sink_overrides.remove(name) {
            return Ok(s);
        }
        if name == "@null" {
            return Ok(Box::new(NullSink::new()));
        }
        if name.starts_with('@') {
            if is_reserved_sink(name) {
                return Err(Error::unsupported(format!(
                    "job: no handler registered for reserved sink {name} (use with_sink_override)"
                )));
            }
            return Err(Error::invalid(format!(
                "job: alias {name} cannot be used as an output sink"
            )));
        }
        // File sink — open a muxer matching the path extension.
        let path = PathBuf::from(name);
        let fout = open_file_write(&path)?;
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .ok_or_else(|| Error::invalid(format!("job: output {name}: no extension")))?;
        let format = self
            .ctx
            .containers
            .container_for_extension(ext)
            .ok_or_else(|| {
                Error::FormatNotFound(format!("no muxer registered for extension .{ext}"))
            })?
            .to_owned();
        let muxer = self.ctx.containers.open_muxer(&format, fout, out_streams)?;
        Ok(Box::new(FileSink::new(path, muxer)))
    }
}

// ───────────────────────── per-track runtime ─────────────────────────

#[derive(Clone, Debug)]
pub(crate) enum StageSpec {
    Decode,
    Filter {
        name: String,
        params: serde_json::Value,
    },
    /// Pixel-format conversion stage (video only). Inserted either from
    /// an explicit `DagNode::PixConvert` or by the auto-insert pass in
    /// [`Executor::build_track_runtime`] when a codec declares
    /// `accepted_pixel_formats`.
    Convert {
        target: PixelFormat,
    },
    Encode {
        codec: String,
        params: serde_json::Value,
    },
}

/// Immutable runtime identity for one routed executor track.
///
/// Decoder capabilities are the registration that actually instantiated
/// successfully after preference filtering and init-time fallback.
#[derive(Clone, Debug)]
pub struct PipelineTrackInfo {
    pub source_uri: String,
    pub source_stream: u32,
    pub media_type: MediaType,
    pub codec_id: CodecId,
    pub copy: bool,
    pub decoder: Option<CodecCapabilities>,
}

/// One track's execution state: decoder + per-frame stage chain +
/// encoder, plus the resolved source URI + selected stream index. Used
/// by both the serial executor and the pipelined runner in
/// [`crate::pipeline`].
pub(crate) struct TrackRuntime {
    pub(crate) source_uri: String,
    pub(crate) selector: ResolvedSelector,
    pub(crate) source_stream: u32,
    pub(crate) kind: MediaType,
    pub(crate) copy: bool,
    pub(crate) stages: Vec<StageSpec>,
    pub(crate) input_params: CodecParameters,
    pub(crate) input_time_base: TimeBase,
    /// Start timestamp advertised by the selected source stream, in
    /// `input_time_base` units. `None` is significant: sources such as
    /// MPEG-TS may not know their first timestamp until packets flow, so
    /// downstream sinks must not invent a zero origin.
    pub(crate) input_start_time: Option<i64>,
    /// Duration advertised by the selected source stream, in
    /// `input_time_base` units. Preserved/rescaled for sink-facing metadata.
    pub(crate) input_duration: Option<i64>,
    pub(crate) decoder: Option<Box<dyn Decoder>>,
    pub(crate) decoder_caps: Option<CodecCapabilities>,
    /// Per-frame stages in order (filters + pixel-format conversions)
    /// between the decoder and the encoder. The pipelined runner
    /// drains this vector to spawn one worker per stage.
    pub(crate) frame_stages: Vec<FrameStage>,
    pub(crate) encoder: Option<Box<dyn Encoder>>,
    pub(crate) encoder_time_base: Option<TimeBase>,
    /// Additional output-stream descriptors synthesised for multi-port
    /// filters on this track (e.g. spectrogram's video-port).
    /// Each extra gets its own [`StreamInfo`] in the sink's `start()`
    /// call, and extra-port frames emitted by `push`/`flush` flow
    /// straight into the sink tagged with their [`MediaType`].
    pub(crate) extra_output_streams: Vec<StreamInfo>,
    /// Per-filter-stage count of extra (non-primary) output ports.
    /// Parallel to `frame_stages` but only lists Filter stages — a
    /// PixConvert stage contributes nothing. Used by the staged
    /// runner to allocate stable stream indices for each filter's
    /// extras.
    pub(crate) extra_output_port_counts: Vec<u32>,
    /// Global index of this track's first auto-attached extra stream
    /// in the sink's final [`StreamInfo`] list. Populated by
    /// [`build_output_streams`] before the pipelined runner spawns.
    pub(crate) extras_base_index: u32,
}

/// A runtime filter instance in `StreamFilter` form. Obtained from the
/// [`FilterRegistry`]; the executor collects emitted frames on every
/// output port and routes them to downstream stages (port 0) or
/// directly to the sink (ports ≥ 1).
pub(crate) struct RuntimeFilter {
    pub(crate) inner: Box<dyn StreamFilter>,
}

/// A per-frame stage in the decoded-frame pipeline. Decoder output
/// flows through these in order to reach the encoder.
pub(crate) enum FrameStage {
    /// Named filter. Multi-port filters (e.g. spectrogram)
    /// emit on port 0 *and* extra ports; the executor separates the
    /// two streams after `push` returns.
    Filter(RuntimeFilter),
    /// Pixel-format conversion — calls
    /// [`oxideav_pixfmt::convert`] on each video frame. Non-video
    /// frames pass through unchanged. Carries the source-side
    /// `FrameInfo` (pixel format + width + height) since that data
    /// no longer lives on `VideoFrame`; we cache it at stage-build
    /// time off the running [`CodecParameters`].
    PixConvert {
        src_info: oxideav_pixfmt::FrameInfo,
        target: PixelFormat,
    },
}

/// Classification of a filter's emitted frames. `primary` goes to the
/// next frame stage (or the encoder); `extras` carry `(media_kind,
/// Frame)` pairs destined for the sink as auto-attached streams.
#[derive(Default)]
pub(crate) struct FilterEmissions {
    pub(crate) primary: Vec<Frame>,
    pub(crate) extras: Vec<(MediaType, Frame)>,
}

/// In-place `FilterContext` used by the executor. Collects emitted
/// frames into an `EmissionBuffer` so the stage worker can route them
/// after `push`/`flush` returns.
pub(crate) struct CollectCtx<'a> {
    pub(crate) port_kinds: &'a [MediaType],
    pub(crate) buf: FilterEmissions,
}

impl<'a> FilterContext for CollectCtx<'a> {
    fn emit(&mut self, port: usize, frame: Frame) -> Result<()> {
        match port {
            0 => self.buf.primary.push(frame),
            p if p < self.port_kinds.len() => {
                self.buf.extras.push((self.port_kinds[p], frame));
            }
            p => {
                return Err(Error::invalid(format!(
                    "filter emitted on port {p} but declared only {} ports",
                    self.port_kinds.len()
                )));
            }
        }
        Ok(())
    }
}

impl TrackRuntime {
    fn new(
        source_uri: String,
        selector: ResolvedSelector,
        kind: MediaType,
        copy: bool,
        stages: Vec<StageSpec>,
    ) -> Self {
        Self {
            source_uri,
            selector,
            source_stream: 0,
            kind,
            copy,
            stages,
            input_params: CodecParameters::audio(CodecId::new("")),
            input_time_base: TimeBase::new(1, 1),
            input_start_time: None,
            input_duration: None,
            decoder: None,
            decoder_caps: None,
            frame_stages: Vec::new(),
            encoder: None,
            encoder_time_base: None,
            extra_output_streams: Vec::new(),
            extra_output_port_counts: Vec::new(),
            extras_base_index: 0,
        }
    }

    /// Clone a pre-instantiate TrackRuntime — only the metadata + stage
    /// list, not the decoder / encoder / filter-instance slots (which
    /// are `None` at this point anyway). Used by
    /// [`expand_all_tracks_pump`] when a `{kind: Unknown, selector: any}`
    /// track needs to fan out into one runtime per source stream.
    fn duplicate_pre_instantiate(&self, kind: MediaType, selector: ResolvedSelector) -> Self {
        assert!(
            self.decoder.is_none() && self.encoder.is_none() && self.frame_stages.is_empty(),
            "duplicate_pre_instantiate called post-instantiate"
        );
        TrackRuntime::new(
            self.source_uri.clone(),
            selector,
            kind,
            self.copy,
            self.stages.clone(),
        )
    }

    fn pipeline_info(&self) -> PipelineTrackInfo {
        PipelineTrackInfo {
            source_uri: self.source_uri.clone(),
            source_stream: self.source_stream,
            media_type: self.kind,
            codec_id: self.input_params.codec_id.clone(),
            copy: self.copy,
            decoder: self.decoder_caps.clone(),
        }
    }

    pub(crate) fn instantiate(
        &mut self,
        codecs: &CodecRegistry,
        preferences: &CodecPreferences,
        ctx: &ExecutionContext,
        filters: &FilterRegistry,
    ) -> Result<()> {
        // Track running frame format through the stage stack so the encoder
        // can be constructed with a realistic parameter set.
        let mut running = self.input_params.clone();
        // Track the running PortSpec that feeds into the next filter. For
        // audio tracks this starts from `input_params`; for video from
        // `input_params`. We rebuild it each iteration so chained filters
        // see the immediately-upstream port, not the original demuxer port.
        let mut extra_stream_index_base: u32 = 1;
        for stage in &self.stages {
            match stage {
                StageSpec::Decode => {
                    if self.decoder.is_none() {
                        let (mut decoder, caps) =
                            make_decoder_with_selection(codecs, &self.input_params, preferences)?;
                        decoder.set_execution_context(ctx);
                        self.decoder = Some(decoder);
                        self.decoder_caps = Some(caps);
                    }
                }
                StageSpec::Filter { name, params } => {
                    let in_port = port_spec_from_params(&running, self.input_time_base);
                    let filter = filters.make(name, params, std::slice::from_ref(&in_port))?;
                    // Inspect output ports: port 0 feeds downstream; every
                    // extra port becomes an auto-attached output stream.
                    let out_ports = filter.output_ports().to_vec();
                    let in_ports = filter.input_ports().to_vec();
                    if out_ports.is_empty() {
                        return Err(Error::invalid(format!(
                            "filter '{name}' declares zero output ports"
                        )));
                    }
                    // Port 0 rewrites `running` when the filter's port-0
                    // output differs from its port-0 input (i.e. the filter
                    // is actually changing shape — resample, resize, edge).
                    // Pass-through port-0 (spectrogram audio, volume) leaves
                    // `running` untouched so encoders see the actual source
                    // params rather than a filter's placeholder.
                    let input_matches_output = !in_ports.is_empty()
                        && port_params_equal(&in_ports[0].params, &out_ports[0].params);
                    if !input_matches_output {
                        running = port_params_to_codec_params(&out_ports[0].params, &running);
                    }
                    // Register StreamInfo for each extra port so the sink
                    // sees them before any frame flows.
                    for (i, port) in out_ports.iter().enumerate().skip(1) {
                        let (params_cp, time_base) = extra_port_stream(&port.params);
                        self.extra_output_streams.push(StreamInfo {
                            index: extra_stream_index_base + (i - 1) as u32,
                            time_base,
                            duration: None,
                            start_time: Some(0),
                            params: params_cp,
                        });
                    }
                    let extra_count = out_ports.len().saturating_sub(1) as u32;
                    extra_stream_index_base += extra_count;
                    self.extra_output_port_counts.push(extra_count);
                    self.frame_stages
                        .push(FrameStage::Filter(RuntimeFilter { inner: filter }));
                }
                StageSpec::Convert { target } => {
                    // Snapshot the running source shape (format + dims)
                    // to thread into pixfmt_convert — those used to live
                    // on VideoFrame but moved to CodecParameters with
                    // the slim.
                    let src_info = oxideav_pixfmt::FrameInfo::new(
                        running.pixel_format.unwrap_or(*target),
                        running.width.unwrap_or(0),
                        running.height.unwrap_or(0),
                    );
                    self.frame_stages.push(FrameStage::PixConvert {
                        src_info,
                        target: *target,
                    });
                    // Propagate the target format into the running
                    // CodecParameters so a downstream encoder sees the
                    // correct `pixel_format`.
                    running.pixel_format = Some(*target);
                }
                StageSpec::Encode { codec, params } => {
                    let mut enc_params = running.clone();
                    enc_params.codec_id = CodecId::new(codec.as_str());
                    // Map a handful of common params directly onto
                    // CodecParameters. Everything else is ignored by the
                    // generic path — codec-specific crates can read extras
                    // via their own param-parsing when we add that layer.
                    if let Some(br) = params.get("bitrate").and_then(|b| b.as_u64()) {
                        enc_params.bit_rate = Some(br);
                    }
                    if let Some(sr) = params.get("sample_rate").and_then(|b| b.as_u64()) {
                        enc_params.sample_rate = Some(sr as u32);
                    }
                    if let Some(ch) = params.get("channels").and_then(|b| b.as_u64()) {
                        enc_params.channels = Some(ch as u16);
                    }
                    if let Some(w) = params.get("width").and_then(|b| b.as_u64()) {
                        enc_params.width = Some(w as u32);
                    }
                    if let Some(h) = params.get("height").and_then(|b| b.as_u64()) {
                        enc_params.height = Some(h as u32);
                    }
                    let mut encoder = make_encoder_with(codecs, &enc_params, preferences)?;
                    encoder.set_execution_context(ctx);
                    let out_params = encoder.output_params().clone();
                    running = out_params.clone();
                    self.encoder_time_base = Some(match out_params.sample_rate {
                        Some(sr) if sr > 0 => TimeBase::new(1, sr as i64),
                        _ => self.input_time_base,
                    });
                    self.encoder = Some(encoder);
                }
            }
        }
        Ok(())
    }

    /// Rewrite `self.stages` to insert `StageSpec::Convert` in front of
    /// any `Encode` whose codec declares a non-empty
    /// `accepted_pixel_formats` set that does not include the currently
    /// running pixel format.
    ///
    /// Must be called after `input_params` has been populated from the
    /// demuxer and before `instantiate`. Audio tracks (where
    /// `input_params.pixel_format` is `None`) are a no-op — the pass
    /// only applies when a concrete source pixel format is available.
    ///
    /// **Caveat:** we assume filters preserve the pixel format
    /// (they do today — only audio filters exist — but this assumption
    /// will have to be revisited when video filters land and can
    /// change the pixel format).
    pub(crate) fn apply_pixel_format_auto_insert(&mut self, codecs: &CodecRegistry) {
        let src_fmt = match self.input_params.pixel_format {
            Some(f) => f,
            None => return,
        };
        let mut rewritten: Vec<StageSpec> = Vec::with_capacity(self.stages.len() + 2);
        let mut running: Option<PixelFormat> = None;
        for stage in std::mem::take(&mut self.stages).into_iter() {
            match &stage {
                StageSpec::Decode => {
                    running = Some(src_fmt);
                    rewritten.push(stage);
                }
                StageSpec::Filter { .. } => {
                    rewritten.push(stage);
                }
                StageSpec::Convert { target } => {
                    running = Some(*target);
                    rewritten.push(stage);
                }
                StageSpec::Encode { codec, .. } => {
                    if let Some(cur_fmt) = running {
                        if let Some(accepted) = codec_accepted_pixel_formats(codecs, codec) {
                            if !accepted.contains(&cur_fmt) {
                                let target = accepted[0];
                                rewritten.push(StageSpec::Convert { target });
                                running = Some(target);
                            }
                        }
                    }
                    rewritten.push(stage);
                }
            }
        }
        self.stages = rewritten;
    }

    pub(crate) fn output_params(&self) -> &CodecParameters {
        if let Some(enc) = &self.encoder {
            enc.output_params()
        } else if let Some(dec) = &self.decoder {
            dec.output_params().unwrap_or(&self.input_params)
        } else {
            &self.input_params
        }
    }

    pub(crate) fn output_time_base(&self) -> TimeBase {
        self.encoder_time_base.unwrap_or(self.input_time_base)
    }

    /// Push one frame straight into this track's frame-stage chain.
    /// Used when the source is a [`SourcePump::Frames`] — there's no
    /// upstream decoder to drive, so we skip directly to `pump_frame`.
    pub(crate) fn feed_frame(
        &mut self,
        frame: Frame,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> StageResult<()> {
        self.pump_frame(FrameLease::from_frame(frame), track_index, sink, stats)
    }

    fn feed_packet(
        &mut self,
        pkt: &Packet,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> StageResult<()> {
        if self.copy {
            // Pure copy: retag stream index + forward.
            let mut out = pkt.clone();
            out.stream_index = track_index;
            sink.write_packet(self.kind, &out)
                .map_err(attribute(FailureStage::Sink, Some(track_index)))?;
            stats.packets_copied += 1;
            return Ok(());
        }
        // Stream frames through `pump_frame` as they're produced rather
        // than buffering them all into a Vec first. Streaming codecs
        // (MOD, S3M, XM tracker codecs; future live RTP / HLS feeds)
        // emit far more than one frame per packet — for songs that loop
        // via Bxx the decoder is effectively infinite. A bounded
        // drain-then-send shape here would never reach the first sink
        // call. Inner-scope borrow on `self.decoder` keeps `pump_frame`'s
        // `&mut self` borrow legal.
        if self.decoder.is_some() {
            if let Some(dec) = &mut self.decoder {
                if let Err(e) = dec.send_packet(pkt) {
                    // Per-packet decoder errors must not abort the stream.
                    // Same rationale as `staged.rs::run_decode_stage`: a
                    // single recoverable bit-stream glitch should be
                    // logged and skipped, not propagated to the executor.
                    stats.packets_skipped += 1;
                    log::warn!(
                        "executor: decoder skipped packet (track {}, pts {:?}): {}",
                        track_index,
                        pkt.pts,
                        e
                    );
                    return Ok(());
                }
            }
            let mut produced_any = false;
            loop {
                let frame = match self.decoder.as_mut().unwrap().receive_frame_lease() {
                    Ok(f) => f,
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => {
                        // No frames produced for this packet — count it as
                        // skipped so the same per-packet tolerance contract
                        // surfaces on `stats.packets_skipped` regardless of
                        // which decoder error branch fired. A receive_frame
                        // error *after* one or more frames already streamed
                        // is treated as end-of-output for this packet only
                        // (the packet wasn't lost — partial output landed).
                        if !produced_any {
                            stats.packets_skipped += 1;
                        }
                        log::warn!(
                            "executor: decoder receive_frame error (track {}, pts {:?}): {}",
                            track_index,
                            pkt.pts,
                            e
                        );
                        break;
                    }
                };
                produced_any = true;
                stats.frames_decoded += 1;
                self.pump_frame(frame, track_index, sink, stats)?;
            }
        }
        Ok(())
    }

    fn pump_frame(
        &mut self,
        frame: FrameLease,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> StageResult<()> {
        let sinkf = attribute(FailureStage::Sink, Some(track_index));
        let encf = attribute(FailureStage::Encode, Some(track_index));

        // The direct player path preserves the decoder's native storage all the
        // way to the sink. No CPU arena copy and no hardware-surface readback.
        if self.frame_stages.is_empty() && self.encoder.is_none() {
            sink.write_frame_lease(self.kind, frame).map_err(&sinkf)?;
            stats.frames_written += 1;
            return Ok(());
        }

        // Filters and encoders still consume the legacy Frame API. Materialise
        // exactly once at that explicit compatibility boundary.
        let convertf = attribute(FailureStage::Convert, Some(track_index));
        let mut frames: Vec<Frame> = vec![frame.into_frame().map_err(&convertf)?];
        for stage in &mut self.frame_stages {
            let stagef = attribute(frame_stage_failure_kind(stage), Some(track_index));
            let mut next = Vec::new();
            for f in frames {
                let produced = run_frame_stage_emit(stage, f).map_err(&stagef)?;
                // Extras (multi-port filter emissions) go straight to the
                // sink as auto-attached streams. Wrap them in a lease so an
                // asynchronous sink need not clone their media buffers.
                for (kind, frm) in produced.extras {
                    sink.write_frame_lease(kind, FrameLease::from_frame(frm))
                        .map_err(&sinkf)?;
                    stats.frames_written += 1;
                }
                next.extend(produced.primary);
            }
            frames = next;
        }

        if let Some(enc) = &mut self.encoder {
            for frame in frames {
                enc.send_frame(&frame).map_err(&encf)?;
                loop {
                    match enc.receive_packet() {
                        Ok(mut p) => {
                            p.stream_index = track_index;
                            sink.write_packet(self.kind, &p).map_err(&sinkf)?;
                            stats.packets_encoded += 1;
                        }
                        Err(Error::NeedMore) | Err(Error::Eof) => break,
                        Err(e) => return Err(encf(e)),
                    }
                }
            }
        } else {
            // Frame stages produced fresh owned frames. Preserve ownership into
            // a player sink rather than borrowing and forcing it to clone.
            for f in frames {
                sink.write_frame_lease(self.kind, FrameLease::from_frame(f))
                    .map_err(&sinkf)?;
                stats.frames_written += 1;
            }
        }
        Ok(())
    }

    fn drain(
        &mut self,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> StageResult<()> {
        let sinkf = attribute(FailureStage::Sink, Some(track_index));
        let encf = attribute(FailureStage::Encode, Some(track_index));
        if self.copy {
            return Ok(());
        }
        // Same streaming shape as `pump_packet` — decoder may emit any
        // number of frames during flush (MOD plays its outro after EOF).
        if self.decoder.is_some() {
            if let Some(dec) = &mut self.decoder {
                if let Err(e) = dec.flush() {
                    log::warn!(
                        "executor: decoder flush error (track {}): {}",
                        track_index,
                        e
                    );
                }
            }
            loop {
                let frame = match self.decoder.as_mut().unwrap().receive_frame_lease() {
                    Ok(f) => f,
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => {
                        log::warn!(
                            "executor: decoder error during EOF drain (track {}): {}",
                            track_index,
                            e
                        );
                        break;
                    }
                };
                stats.frames_decoded += 1;
                self.pump_frame(frame, track_index, sink, stats)?;
            }
        }
        // Flush frame stages. Filters may hold internal buffers
        // (resampler tail, noise-gate decay); pixel-format converts
        // are stateless so they drain trivially. After each stage
        // flushes, push its residual frames through the remaining
        // stages downstream.
        let mut tail: Vec<Frame> = Vec::new();
        for i in 0..self.frame_stages.len() {
            let stagef = attribute(
                frame_stage_failure_kind(&self.frame_stages[i]),
                Some(track_index),
            );
            let flushed = flush_frame_stage_emit(&mut self.frame_stages[i]).map_err(&stagef)?;
            // Extras from a flushing filter go straight to the sink.
            for (kind, frm) in flushed.extras {
                sink.write_frame_lease(kind, FrameLease::from_frame(frm))
                    .map_err(&sinkf)?;
                stats.frames_written += 1;
            }
            let mut primary = flushed.primary;
            // Push the flushed frames through the remaining downstream
            // stages so a later pixel-format convert still sees them.
            for j in (i + 1)..self.frame_stages.len() {
                let stagef = attribute(
                    frame_stage_failure_kind(&self.frame_stages[j]),
                    Some(track_index),
                );
                let mut next: Vec<Frame> = Vec::new();
                for f in primary.drain(..) {
                    let produced =
                        run_frame_stage_emit(&mut self.frame_stages[j], f).map_err(&stagef)?;
                    for (kind, frm) in produced.extras {
                        sink.write_frame_lease(kind, FrameLease::from_frame(frm))
                            .map_err(&sinkf)?;
                        stats.frames_written += 1;
                    }
                    next.extend(produced.primary);
                }
                primary = next;
            }
            tail.extend(primary);
        }
        if let Some(enc) = &mut self.encoder {
            for frame in tail {
                enc.send_frame(&frame).map_err(&encf)?;
                loop {
                    match enc.receive_packet() {
                        Ok(mut p) => {
                            p.stream_index = track_index;
                            sink.write_packet(self.kind, &p).map_err(&sinkf)?;
                            stats.packets_encoded += 1;
                        }
                        Err(Error::NeedMore) | Err(Error::Eof) => break,
                        Err(e) => return Err(encf(e)),
                    }
                }
            }
            enc.flush().map_err(&encf)?;
            loop {
                match enc.receive_packet() {
                    Ok(mut p) => {
                        p.stream_index = track_index;
                        sink.write_packet(self.kind, &p).map_err(&sinkf)?;
                        stats.packets_encoded += 1;
                    }
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => return Err(encf(e)),
                }
            }
        } else {
            for f in tail {
                sink.write_frame_lease(self.kind, FrameLease::from_frame(f))
                    .map_err(&sinkf)?;
                stats.frames_written += 1;
            }
        }
        Ok(())
    }
}

/// Map a [`FrameStage`] variant to its [`FailureStage`] attribution.
pub(crate) fn frame_stage_failure_kind(stage: &FrameStage) -> FailureStage {
    match stage {
        FrameStage::Filter(_) => FailureStage::Filter,
        FrameStage::PixConvert { .. } => FailureStage::Convert,
    }
}

// `drain_decoder` was removed — buffering an entire decoder emission
// into a Vec is unsound for streaming codecs (MOD/S3M/XM tracker codecs
// emit until song end, songs with Bxx loops are effectively infinite),
// and it deferred all sink writes until decode completed even for
// finite codecs. Both call sites (TrackRuntime::pump_packet + drain,
// staged::run_decode_stage) now stream frames through the sink as they
// land, which both back-pressures naturally and keeps memory bounded.

/// Drive one frame through a [`FrameStage`] and return the full
/// primary + extras emission set. Pixel-format converts on a
/// non-video frame pass through unchanged; multi-port filters
/// (spectrogram) put port-1+ frames into `extras` for the caller to
/// route straight to the sink as an auto-attached output stream.
pub(crate) fn run_frame_stage_emit(
    stage: &mut FrameStage,
    frame: Frame,
) -> Result<FilterEmissions> {
    match stage {
        FrameStage::Filter(f) => run_filter(f, frame),
        FrameStage::PixConvert { src_info, target } => match frame {
            Frame::Video(v) => {
                let out = pixfmt_convert(&v, *src_info, *target, &ConvertOptions::default())?;
                Ok(FilterEmissions {
                    primary: vec![Frame::Video(out)],
                    extras: Vec::new(),
                })
            }
            other => Ok(FilterEmissions {
                primary: vec![other],
                extras: Vec::new(),
            }),
        },
    }
}

/// Flush any residual frames held inside a [`FrameStage`]. Audio
/// filters may have tail samples; pixel-format converts are stateless
/// and return an empty [`FilterEmissions`].
pub(crate) fn flush_frame_stage_emit(stage: &mut FrameStage) -> Result<FilterEmissions> {
    match stage {
        FrameStage::Filter(f) => flush_filter(f),
        FrameStage::PixConvert { .. } => Ok(FilterEmissions::default()),
    }
}

pub(crate) fn run_filter(filter: &mut RuntimeFilter, frame: Frame) -> Result<FilterEmissions> {
    let port_kinds: Vec<MediaType> = filter.inner.output_ports().iter().map(|p| p.kind).collect();
    let mut ctx = CollectCtx {
        port_kinds: &port_kinds,
        buf: FilterEmissions::default(),
    };
    filter.inner.push(&mut ctx, 0, &frame)?;
    Ok(ctx.buf)
}

pub(crate) fn flush_filter(filter: &mut RuntimeFilter) -> Result<FilterEmissions> {
    let port_kinds: Vec<MediaType> = filter.inner.output_ports().iter().map(|p| p.kind).collect();
    let mut ctx = CollectCtx {
        port_kinds: &port_kinds,
        buf: FilterEmissions::default(),
    };
    filter.inner.flush(&mut ctx)?;
    Ok(ctx.buf)
}

/// True when two [`PortParams`] describe the same concrete shape (same
/// sample rate/channels/format for audio, same dimensions/pix-fmt/time
/// base for video). Used to detect pass-through port 0 so we don't
/// overwrite the running `CodecParameters` with a filter's placeholder
/// input params.
pub(crate) fn port_params_equal(a: &PortParams, b: &PortParams) -> bool {
    match (a, b) {
        (
            PortParams::Audio {
                sample_rate: ar,
                channels: ac,
                format: af,
            },
            PortParams::Audio {
                sample_rate: br,
                channels: bc,
                format: bf,
            },
        ) => ar == br && ac == bc && af == bf,
        (
            PortParams::Video {
                width: aw,
                height: ah,
                format: af,
                time_base: atb,
            },
            PortParams::Video {
                width: bw,
                height: bh,
                format: bf,
                time_base: btb,
            },
        ) => aw == bw && ah == bh && af == bf && atb == btb,
        (PortParams::Subtitle, PortParams::Subtitle) => true,
        (PortParams::Metadata, PortParams::Metadata) => true,
        _ => false,
    }
}

/// Derive a [`PortSpec`] describing the current running format inside
/// the stage stack. Only Audio / Video variants are produced today.
/// Expand `all:`-originated tracks (kind == `MediaType::Unknown`,
/// selector without an explicit kind constraint) into one
/// [`TrackRuntime`] per source stream exposed by the opened source.
///
/// The DAG builder produces a single MuxTrack per `all:` entry; it
/// can't know the source's stream layout statically, so it leaves the
/// kind as `Unknown` and the selector as "any". The executor's prep
/// phase (both serial and pipelined) calls this after opening
/// sources so the fan-out sees the authoritative stream list.
///
/// Tracks with an explicit kind (from `audio:` / `video:` /
/// `subtitle:` lists) pass through unchanged.
pub(crate) fn expand_all_tracks_pump(
    pipelines: Vec<TrackRuntime>,
    sources_by_uri: &HashMap<String, SourcePump>,
) -> Vec<TrackRuntime> {
    let mut out: Vec<TrackRuntime> = Vec::with_capacity(pipelines.len());
    for pl in pipelines {
        match sources_by_uri.get(&pl.source_uri) {
            Some(pump) => fan_out_one(pl, pump.streams(), &mut out),
            None => out.push(pl),
        }
    }
    out
}

/// Fan one track runtime out over `streams` if it needs expansion,
/// else pass it through unchanged. Factored out of
/// [`expand_all_tracks_pump`] so the fan-out rule lives in one place.
///
/// Each duplicate's selector is pinned to `(kind, per-kind ordinal)` —
/// [`select_stream`]'s `index` counts within the pool of streams
/// matching `kind`, so the ordinal restarts at 0 for every media type.
/// Pinning the ordinal (rather than leaving `index: None`) is what
/// makes two source streams of the SAME kind resolve to two distinct
/// runtimes: with `index: None` every duplicate collapsed onto the
/// first matching stream, so the second audio stream of a dual-audio
/// source was silently replaced by a copy of the first.
fn fan_out_one(pl: TrackRuntime, streams: &[StreamInfo], out: &mut Vec<TrackRuntime>) {
    let needs_expand =
        pl.kind == MediaType::Unknown && pl.selector.kind.is_none() && pl.selector.index.is_none();
    if !needs_expand || streams.is_empty() {
        out.push(pl);
        return;
    }
    let mut per_kind: HashMap<MediaType, u32> = HashMap::new();
    for s in streams {
        let kind = s.params.media_type;
        let ordinal = per_kind.entry(kind).or_insert(0);
        let selector = ResolvedSelector {
            kind: Some(kind),
            index: Some(*ordinal),
        };
        *ordinal += 1;
        out.push(pl.duplicate_pre_instantiate(kind, selector));
    }
}

pub(crate) fn port_spec_from_params(cp: &CodecParameters, tb: TimeBase) -> PortSpec {
    match cp.media_type {
        MediaType::Audio => PortSpec::audio(
            "in",
            cp.sample_rate.unwrap_or(48_000),
            cp.channels.unwrap_or(2),
            cp.sample_format.unwrap_or(SampleFormat::F32),
        ),
        _ => PortSpec::video(
            "in",
            cp.width.unwrap_or(0),
            cp.height.unwrap_or(0),
            cp.pixel_format.unwrap_or(PixelFormat::Yuv420P),
            tb,
        ),
    }
}

/// Merge a filter output port's [`PortParams`] into a running
/// [`CodecParameters`]. The codec id is left unchanged — it describes
/// *packet-level* codec, while the port describes *frame-level*
/// shape. Downstream encoders overwrite codec_id from the track
/// spec's `codec` field.
pub(crate) fn port_params_to_codec_params(
    port: &PortParams,
    prev: &CodecParameters,
) -> CodecParameters {
    let mut cp = prev.clone();
    match port {
        PortParams::Audio {
            sample_rate,
            channels,
            format,
        } => {
            cp.media_type = MediaType::Audio;
            cp.sample_rate = Some(*sample_rate);
            cp.channels = Some(*channels);
            cp.sample_format = Some(*format);
        }
        PortParams::Video {
            width,
            height,
            format,
            ..
        } => {
            cp.media_type = MediaType::Video;
            cp.width = Some(*width);
            cp.height = Some(*height);
            cp.pixel_format = Some(*format);
        }
        PortParams::Subtitle => cp.media_type = MediaType::Subtitle,
        PortParams::Metadata => cp.media_type = MediaType::Data,
    }
    cp
}

/// Build a `(CodecParameters, TimeBase)` pair for an extra (non-primary)
/// filter output port. The sink sees this as a raw frame stream —
/// `codec_id` is set to `"rawaudio"` / `"rawvideo"` matching the
/// convention used by `@display`.
pub(crate) fn extra_port_stream(port: &PortParams) -> (CodecParameters, TimeBase) {
    match port {
        PortParams::Audio {
            sample_rate,
            channels,
            format,
        } => {
            let mut cp = CodecParameters::audio(CodecId::new("rawaudio"));
            cp.sample_rate = Some(*sample_rate);
            cp.channels = Some(*channels);
            cp.sample_format = Some(*format);
            let tb = TimeBase::new(1, (*sample_rate as i64).max(1));
            (cp, tb)
        }
        PortParams::Video {
            width,
            height,
            format,
            time_base,
        } => {
            let mut cp = CodecParameters::video(CodecId::new("rawvideo"));
            cp.width = Some(*width);
            cp.height = Some(*height);
            cp.pixel_format = Some(*format);
            // Frame rate is the reciprocal of the time base.
            let tb_rat = time_base.as_rational();
            cp.frame_rate = Some(Rational::new(tb_rat.den, tb_rat.num));
            (cp, *time_base)
        }
        PortParams::Subtitle => (
            CodecParameters::audio(CodecId::new("rawsubtitle")),
            TimeBase::new(1, 1000),
        ),
        PortParams::Metadata => (
            CodecParameters::audio(CodecId::new("rawdata")),
            TimeBase::new(1, 1),
        ),
    }
}

/// Assemble the final sink-facing [`StreamInfo`] list from a set of
/// track runtimes. Primary streams come first (one per track in
/// declaration order), followed by any auto-attached extras declared
/// by multi-port filters. Indices are renumbered to be consecutive so
/// the sink sees a dense 0..N layout.
///
/// Also stamps `extras_base_index` on each [`TrackRuntime`] so the
/// staged runner can tag extra-port frames with the right stream
/// index at emission time.
pub(crate) fn build_output_streams(pipelines: &mut [TrackRuntime]) -> Vec<StreamInfo> {
    let mut out = Vec::with_capacity(pipelines.len());
    for (i, pl) in pipelines.iter().enumerate() {
        let output_time_base = pl.output_time_base();
        out.push(StreamInfo {
            index: i as u32,
            time_base: output_time_base,
            duration: pl.input_duration.and_then(|duration| {
                pl.input_time_base
                    .rescale_checked(duration, output_time_base)
            }),
            start_time: pl
                .input_start_time
                .and_then(|start| pl.input_time_base.rescale_checked(start, output_time_base)),
            params: pl.output_params().clone(),
        });
    }
    let mut next_idx = pipelines.len() as u32;
    for pl in pipelines {
        pl.extras_base_index = next_idx;
        for extra in &pl.extra_output_streams {
            let mut e = extra.clone();
            e.index = next_idx;
            next_idx += 1;
            out.push(e);
        }
    }
    out
}

pub(crate) fn select_stream(streams: &[StreamInfo], sel: &ResolvedSelector) -> Result<u32> {
    let filtered: Vec<&StreamInfo> = streams
        .iter()
        .filter(|s| match sel.kind {
            Some(k) => s.params.media_type == k,
            None => true,
        })
        .collect();
    if filtered.is_empty() {
        return Err(Error::invalid(format!(
            "job: no streams match selector {sel:?}"
        )));
    }
    let idx = sel.index.unwrap_or(0) as usize;
    let picked = filtered
        .get(idx)
        .ok_or_else(|| Error::invalid(format!("job: selector index {idx} out of range")))?;
    Ok(picked.index)
}

pub(crate) fn ext_from_uri(uri: &str) -> Option<String> {
    let last = uri.rsplit('/').next().unwrap_or(uri);
    let last = last.split('?').next().unwrap_or(last);
    let dot = last.rfind('.')?;
    Some(last[dot + 1..].to_ascii_lowercase())
}

// ───────────────────────── handle ─────────────────────────

/// Owned bundle of everything `staged::run_pipelined_with_control` needs,
/// produced synchronously by [`Executor::prepare_pipelined_run`] so the
/// caller can either run it inline ([`Executor::run`]) or hand it to a
/// background thread ([`Executor::spawn`]). Holds no `'a`-borrowed
/// fields — moves freely across threads.
pub(crate) struct PreparedRun {
    /// The output key this run belongs to (`"out.mp4"`, `"@display"`,
    /// …) — carried so failure attribution on the background-thread
    /// path ([`ExecutorHandle::stop_reporting`]) can name the output.
    pub(crate) output_name: String,
    pub(crate) pipelines: Vec<TrackRuntime>,
    pub(crate) sources_by_uri: HashMap<String, SourcePump>,
    pub(crate) sink: Box<dyn JobSink + Send>,
    pub(crate) out_streams: Vec<StreamInfo>,
    /// Per-track channel-depth budget threaded from the [`Executor`]
    /// onto the background thread used by [`ExecutorHandle`] and the
    /// `run_output_pipelined` call site. `None` keeps the defaults.
    pub(crate) channel_caps: Option<staged::ChannelCaps>,
    /// Aggregate byte ceiling on the demuxer→worker packet queues.
    /// `0` keeps the byte ceiling disabled. Threaded alongside
    /// `channel_caps`.
    pub(crate) max_queue_bytes: u64,
    /// Failed-output disposal opt-in, threaded from
    /// [`Executor::with_discard_failed_outputs`]: on failure the
    /// pipelined runner calls [`JobSink::abandon`] on the sink instead
    /// of dropping it silently.
    pub(crate) discard_on_failure: bool,
}

/// Live handle to a background-running [`Executor`]. Returned by
/// [`Executor::spawn`]; supports seek, abort, and progress polling.
///
/// Dropping the handle without [`Self::stop`] sets the abort flag and
/// detaches the join handle — the worker threads tear down in the
/// background. Use [`Self::stop`] to wait for clean exit + recover
/// the [`ExecutorStats`].
pub struct ExecutorHandle {
    abort: std::sync::Arc<crate::staged::AbortState>,
    seek_tx: std::sync::mpsc::Sender<crate::staged::SeekCmd>,
    /// Monotonic counter incremented inside
    /// [`Self::seek_with_generation`] before each `SeekCmd` is queued.
    /// The value stamped on the command is what the demuxer copies
    /// verbatim into the resulting `BarrierKind::Seek*` — the handle
    /// and the barrier are guaranteed to use the same `generation` so
    /// callers can correlate dispatches with their barriers.
    next_generation: std::sync::Arc<std::sync::atomic::AtomicU32>,
    progress_rx: std::sync::mpsc::Receiver<crate::staged::Progress>,
    join: Option<std::thread::JoinHandle<StageResult<ExecutorStats>>>,
    finished: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Output key of the (single) spawned output, for
    /// [`Self::stop_reporting`]'s failure attribution.
    output_name: String,
    pipeline_tracks: Vec<PipelineTrackInfo>,
}

impl ExecutorHandle {
    pub(crate) fn start(prep: PreparedRun) -> Self {
        let abort = crate::staged::AbortState::new();
        let (seek_tx, seek_rx) = std::sync::mpsc::channel::<crate::staged::SeekCmd>();
        let (progress_tx, progress_rx) =
            std::sync::mpsc::sync_channel::<crate::staged::Progress>(64);
        let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let abort_t = abort.clone();
        let finished_t = finished.clone();
        let caps = prep.channel_caps;
        let max_queue_bytes = prep.max_queue_bytes;
        let discard_on_failure = prep.discard_on_failure;
        let output_name = prep.output_name.clone();
        let pipeline_tracks = prep
            .pipelines
            .iter()
            .map(TrackRuntime::pipeline_info)
            .collect();
        let join = std::thread::Builder::new()
            .name("oxideav-pipeline-exec".into())
            .spawn(move || {
                let result = crate::staged::run_pipelined_with_control(
                    prep.pipelines,
                    prep.sources_by_uri,
                    prep.sink,
                    prep.out_streams,
                    crate::staged::PipelineControl {
                        seek_rx: Some(seek_rx),
                        progress_tx: Some(progress_tx),
                        abort: Some(abort_t),
                        caps,
                        max_queue_bytes,
                        discard_on_failure,
                    },
                );
                finished_t.store(true, std::sync::atomic::Ordering::SeqCst);
                result
            })
            .expect("oxideav-pipeline: failed to spawn executor thread");
        Self {
            abort,
            seek_tx,
            next_generation: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
            progress_rx,
            join: Some(join),
            finished,
            output_name,
            pipeline_tracks,
        }
    }

    /// Immutable runtime track identities captured after decoder
    /// selection and before worker threads start.
    pub fn pipeline_tracks(&self) -> &[PipelineTrackInfo] {
        &self.pipeline_tracks
    }

    /// Issue a seek to `(stream_idx, pts)` in `time_base` units. The
    /// source-pump thread receives the command, calls `demuxer.seek_to`
    /// (bytes-shape sources), then broadcasts exactly one barrier on
    /// every route:
    ///
    /// * [`crate::BarrierKind::SeekFlush`] if the demuxer reported a
    ///   successful seek. Workers reset codec / filter state, the
    ///   engine re-anchors its master clock at the landing pts.
    /// * [`crate::BarrierKind::SeekRejected`] if `demuxer.seek_to`
    ///   returned an error (Unsupported, no SEEKTABLE, etc). The
    ///   pipeline keeps producing packets from where it was; the
    ///   engine should disable seek UI for the session. Packet- and
    ///   frame-shape sources have no seek surface at all, so every
    ///   seek against them is answered with `SeekRejected` too.
    ///
    /// Callers must therefore handle BOTH barrier kinds when matching
    /// on `generation`. The send itself only fails if the executor
    /// thread already exited.
    ///
    /// This convenience wrapper discards the generation that the
    /// handle assigns; prefer [`Self::seek_with_generation`] if you
    /// need to correlate `seek` dispatches with the resulting barrier
    /// (i.e. ignore stale pre-seek payloads still in flight, or detect
    /// dropped seeks under a rapid burst).
    pub fn seek(&self, stream_idx: u32, pts: i64, time_base: oxideav_core::TimeBase) -> Result<()> {
        self.seek_with_generation(stream_idx, pts, time_base)
            .map(|_| ())
    }

    /// Issue a seek and return the `generation` value the handle
    /// assigned. The matching `BarrierKind::SeekFlush { generation, .. }`
    /// or `BarrierKind::SeekRejected { generation }` will surface on
    /// the sink carrying exactly this value, so engines can correlate
    /// `seek_with_generation()` calls with their barriers and discard
    /// any payloads/barriers from earlier generations as stale.
    ///
    /// Generations are assigned by an internal `AtomicU32` counter
    /// (starting at 1; wrapping at `u32::MAX`) so concurrent callers
    /// on the same handle each get a unique value. The demuxer copies
    /// the value verbatim into the resulting barrier — no separate
    /// counter is maintained on the demuxer side, so the handle's
    /// return value and the barrier's `generation` field are
    /// guaranteed to match.
    ///
    /// Returns the assigned generation on success, or an error if the
    /// executor thread has already exited (e.g. EOF was reached
    /// concurrently with the seek dispatch).
    pub fn seek_with_generation(
        &self,
        stream_idx: u32,
        pts: i64,
        time_base: oxideav_core::TimeBase,
    ) -> Result<u32> {
        // `fetch_add` returns the value BEFORE the add, so the very
        // first call returns 0 — but the contract is "generations
        // start at 1" (mirroring the demuxer's pre-fix behaviour and
        // matching every existing test). Pre-increment to keep parity
        // with the prior demuxer-side counter that called
        // `wrapping_add(1)` before stamping the barrier.
        let generation = self
            .next_generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .wrapping_add(1);
        self.seek_tx
            .send(crate::staged::SeekCmd {
                stream_idx,
                pts,
                time_base,
                generation,
            })
            .map_err(|_| Error::other("ExecutorHandle: seek channel closed (executor exited)"))?;
        Ok(generation)
    }

    /// Non-blocking poll of the most recent progress message. Returns
    /// `None` if no update has arrived since the last poll.
    pub fn try_progress(&self) -> Option<crate::staged::Progress> {
        let mut latest = None;
        while let Ok(p) = self.progress_rx.try_recv() {
            latest = Some(p);
        }
        latest
    }

    /// True once the background thread has returned (EOF, abort, or
    /// error). Safe to poll concurrently with [`Self::try_progress`].
    pub fn has_finished(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Set the abort flag without joining. Useful when the engine is
    /// quitting and wants the worker to wind down while it tears down
    /// its driver. Pair with [`Self::stop`] to actually collect the
    /// thread's result.
    pub fn request_abort(&self) {
        self.abort.request_abort();
    }

    /// Set the abort flag, join the worker thread, and return its
    /// final stats (or the first error it observed).
    ///
    /// A clean stop (no worker error recorded) still finalises the
    /// sink — the trailer is written and `Ok(stats)` returns. Use
    /// [`Self::stop_reporting`] to learn *where* a failure fired; both
    /// surfaces observe the same original error.
    pub fn stop(self) -> Result<ExecutorStats> {
        self.stop_reporting().map_err(Error::from)
    }

    /// Like [`Self::stop`], but a failure returns a [`RunFailure`]
    /// carrying the attribution (output key, [`FailureStage`], track)
    /// alongside the original error — the background-thread analogue
    /// of [`Executor::run_reporting`].
    pub fn stop_reporting(mut self) -> std::result::Result<ExecutorStats, RunFailure> {
        self.abort.request_abort();
        let name = std::mem::take(&mut self.output_name);
        match self.join.take() {
            Some(h) => match h.join() {
                Ok(r) => r.map_err(|f| RunFailure::for_output(&name, f)),
                Err(_) => Err(RunFailure::for_output(
                    &name,
                    StageFailure::new(
                        FailureStage::Sink,
                        None,
                        Error::other("ExecutorHandle: worker thread panicked"),
                    ),
                )),
            },
            None => Err(RunFailure::for_output(
                &name,
                StageFailure::new(
                    FailureStage::Prepare,
                    None,
                    Error::other("ExecutorHandle: stop() called twice or after drop"),
                ),
            )),
        }
    }
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        self.abort.request_abort();
        // Detach: the background thread observes the abort flag and
        // tears down. We don't join here because the engine may have
        // already called `stop()` (which moved the join handle out).
    }
}

// ───────────────────────── stats ─────────────────────────

#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorStats {
    pub packets_read: u64,
    pub packets_copied: u64,
    pub packets_encoded: u64,
    pub frames_decoded: u64,
    pub frames_written: u64,
    /// Decoder skipped packets — the per-packet error-tolerance contract
    /// pinned by `tests/decoder_error_tolerance.rs` lets a recoverable
    /// `send_packet` / `receive_frame` error on a single packet log + skip
    /// instead of killing the stream. This counter records every such
    /// event so a CLI can report "decoded N packets, skipped K" without
    /// scraping the stderr log. Always `0` on a clean stream and on
    /// copy-only outputs (no decoder instantiated).
    pub packets_skipped: u64,
}

impl ExecutorStats {
    pub(crate) fn merge(&mut self, other: &Self) {
        self.packets_read += other.packets_read;
        self.packets_copied += other.packets_copied;
        self.packets_encoded += other.packets_encoded;
        self.frames_decoded += other.frames_decoded;
        self.frames_written += other.frames_written;
        self.packets_skipped += other.packets_skipped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_from_uri_basic() {
        assert_eq!(ext_from_uri("foo.mp3").as_deref(), Some("mp3"));
        assert_eq!(
            ext_from_uri("https://x/y.mkv?token=1").as_deref(),
            Some("mkv")
        );
        assert_eq!(ext_from_uri("/no/ext"), None);
    }

    #[test]
    fn output_stream_preserves_unknown_source_start_time() {
        let mut track = TrackRuntime::new(
            "test://video".to_string(),
            ResolvedSelector::any(),
            MediaType::Video,
            false,
            Vec::new(),
        );
        track.input_time_base = TimeBase::new(1, 90_000);
        track.input_start_time = None;

        let streams = build_output_streams(std::slice::from_mut(&mut track));
        assert_eq!(streams[0].start_time, None);
    }

    #[test]
    fn output_stream_rescales_known_source_start_time() {
        let mut track = TrackRuntime::new(
            "test://audio".to_string(),
            ResolvedSelector::any(),
            MediaType::Audio,
            false,
            Vec::new(),
        );
        track.input_time_base = TimeBase::new(1, 90_000);
        track.input_start_time = Some(6_300_000);
        track.input_duration = Some(540_000);
        track.encoder_time_base = Some(TimeBase::new(1, 1_000));

        let streams = build_output_streams(std::slice::from_mut(&mut track));
        assert_eq!(streams[0].time_base, TimeBase::new(1, 1_000));
        assert_eq!(streams[0].start_time, Some(70_000));
        assert_eq!(streams[0].duration, Some(6_000));
    }

    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use oxideav_core::{HardwareVideoFrame, HardwareVideoFrameStorage, VideoFrame, VideoPlane};

    struct CountingHardwareStorage {
        materializations: Arc<AtomicUsize>,
    }

    impl HardwareVideoFrameStorage for CountingHardwareStorage {
        fn backend(&self) -> &'static str {
            "test-hw"
        }

        fn width(&self) -> u32 {
            2
        }

        fn height(&self) -> u32 {
            2
        }

        fn pixel_format(&self) -> PixelFormat {
            PixelFormat::Yuv420P
        }

        fn pts(&self) -> Option<i64> {
            Some(7)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn materialize(&self) -> Result<VideoFrame> {
            self.materializations.fetch_add(1, Ordering::SeqCst);
            Ok(VideoFrame {
                pts: Some(7),
                planes: vec![
                    VideoPlane {
                        stride: 2,
                        data: vec![16, 32, 48, 64],
                    },
                    VideoPlane {
                        stride: 1,
                        data: vec![128],
                    },
                    VideoPlane {
                        stride: 1,
                        data: vec![128],
                    },
                ],
            })
        }
    }

    #[derive(Default)]
    struct LeaseObservingSink {
        hardware_writes: usize,
        owned_writes: usize,
        legacy_writes: usize,
    }

    impl JobSink for LeaseObservingSink {
        fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
            Ok(())
        }

        fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
            Ok(())
        }

        fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
            self.legacy_writes += 1;
            Ok(())
        }

        fn write_frame_lease(&mut self, _kind: MediaType, lease: FrameLease) -> Result<()> {
            if lease.is_hardware_video() {
                self.hardware_writes += 1;
            }
            if lease.as_frame().is_some() {
                self.owned_writes += 1;
            }
            Ok(())
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn test_hardware_lease(materializations: Arc<AtomicUsize>) -> FrameLease {
        FrameLease::from_hardware_video(HardwareVideoFrame::new(CountingHardwareStorage {
            materializations,
        }))
    }

    #[test]
    fn direct_player_path_preserves_hardware_lease_without_materializing() {
        let materializations = Arc::new(AtomicUsize::new(0));
        let lease = test_hardware_lease(Arc::clone(&materializations));
        let mut track = TrackRuntime::new(
            "test://video".to_string(),
            ResolvedSelector::any(),
            MediaType::Video,
            false,
            Vec::new(),
        );
        let mut sink = LeaseObservingSink::default();
        let mut stats = ExecutorStats::default();

        track
            .pump_frame(lease, 0, &mut sink, &mut stats)
            .expect("direct frame lease must reach the sink");

        assert_eq!(materializations.load(Ordering::SeqCst), 0);
        assert_eq!(sink.hardware_writes, 1);
        assert_eq!(sink.owned_writes, 0);
        assert_eq!(sink.legacy_writes, 0);
        assert_eq!(stats.frames_written, 1);
    }

    #[test]
    fn legacy_frame_stage_materializes_hardware_lease_once() {
        let materializations = Arc::new(AtomicUsize::new(0));
        let lease = test_hardware_lease(Arc::clone(&materializations));
        let mut track = TrackRuntime::new(
            "test://video".to_string(),
            ResolvedSelector::any(),
            MediaType::Video,
            false,
            Vec::new(),
        );
        track.frame_stages.push(FrameStage::PixConvert {
            src_info: oxideav_pixfmt::FrameInfo::new(PixelFormat::Yuv420P, 2, 2),
            target: PixelFormat::Rgba,
        });
        let mut sink = LeaseObservingSink::default();
        let mut stats = ExecutorStats::default();

        track
            .pump_frame(lease, 0, &mut sink, &mut stats)
            .expect("hardware lease should materialize at the legacy frame stage");

        assert_eq!(materializations.load(Ordering::SeqCst), 1);
        assert_eq!(sink.hardware_writes, 0);
        assert_eq!(sink.owned_writes, 1);
        assert_eq!(sink.legacy_writes, 0);
        assert_eq!(stats.frames_written, 1);
    }

    // Legacy `build_video_filter` / `build_audio_filter` unit tests
    // were removed when filter dispatch moved to `FilterRegistry`.
    // See `filter_registry::tests::*` for the current equivalents.

    // ───────────────── Phase C-3c: render_source_factory ─────────────────
    //
    // The wiring under test is `Executor::resolve_source_shapes`:
    //  - With NO factory installed, a `DagNode::Render3D` leaf fails with
    //    `Error::Unsupported` pointing at `with_render_source_factory`.
    //  - With a stub factory installed, the DAG node is rewritten to
    //    `DagNode::FrameSource { source }` and the returned map carries a
    //    `SourcePump::Frames { source, streams }` whose `streams` was
    //    synthesised from the stub's `params()`.
    //
    // The tests bypass `Job::from_json` (no JSON producer for `Render3D`
    // exists yet) and build the DAG by hand using `Dag::push`.

    use crate::dag::NodeId;
    use oxideav_core::{AudioFrame, CodecId, CodecParameters, SampleFormat};

    /// Minimal stub: one audio frame, then EOF. Tiny on purpose so the
    /// test asserts the wiring (factory invocation + DAG rewrite), not
    /// codec-shaped behaviour.
    struct StubFrameSource {
        params: CodecParameters,
        emitted: bool,
    }

    impl StubFrameSource {
        fn new() -> Self {
            let mut params = CodecParameters::audio(CodecId::new("pcm_s16le"));
            params.sample_rate = Some(8_000);
            params.channels = Some(1);
            params.sample_format = Some(SampleFormat::S16);
            Self {
                params,
                emitted: false,
            }
        }
    }

    impl FrameSource for StubFrameSource {
        fn params(&self) -> &CodecParameters {
            &self.params
        }
        fn next_frame(&mut self) -> Result<Frame> {
            if self.emitted {
                return Err(Error::Eof);
            }
            self.emitted = true;
            Ok(Frame::Audio(AudioFrame {
                samples: 1,
                pts: Some(0),
                data: vec![vec![0u8, 0u8]],
            }))
        }
    }

    /// Hand-build a one-node DAG containing exactly a `Render3D` leaf
    /// plus a stub `Mux` root, so `resolve_source_shapes` has something
    /// to walk. We don't run `Executor::run` end-to-end because that
    /// would also need a real codec registry — `resolve_source_shapes`
    /// is the wiring under test here.
    fn dag_with_render3d(source: &str, backend: &str) -> Dag {
        let mut dag = Dag::default();
        let render = dag.push(DagNode::Render3D {
            source: source.to_string(),
            backend: backend.to_string(),
            opts: serde_json::json!({"width": 4, "height": 4}),
        });
        let mux = dag.push(DagNode::Mux {
            target: "out".to_string(),
            tracks: vec![MuxTrack {
                kind: oxideav_core::MediaType::Video,
                upstream: render,
                copy: false,
            }],
        });
        dag.roots.insert("out".to_string(), mux);
        dag
    }

    #[test]
    fn render3d_without_factory_errors_with_pointer_to_install_api() {
        // No `with_render_source_factory` call → `resolve_source_shapes`
        // must report `Unsupported` and name the API the caller should
        // reach for. We use `Job::from_json` only to obtain a valid
        // `Job` value to satisfy `Executor::new`; the actual DAG under
        // test is built by hand below.
        let job = Job::from_json(r#"{"out.wav": {"audio": [{"from": "in.wav"}]}}"#).unwrap();
        let ctx = RuntimeContext::new();
        let executor = Executor::new(&job, &ctx);
        let mut dag = dag_with_render3d("scene.gltf", "scanline");
        let err = match executor.resolve_source_shapes(&mut dag) {
            Ok(_) => panic!("resolve_source_shapes should fail with no factory"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("render_source_factory"),
            "error message must name the factory slot, got: {msg}"
        );
        assert!(
            msg.contains("with_render_source_factory"),
            "error message must point at the install API, got: {msg}"
        );
        // Confirm error kind is Unsupported, not Other / Invalid.
        match err {
            Error::Unsupported(_) => {}
            other => panic!("expected Error::Unsupported, got {other:?}"),
        }
        // DAG must NOT have been mutated when resolution fails.
        match dag.node(NodeId(0)) {
            DagNode::Render3D {
                source, backend, ..
            } => {
                assert_eq!(source, "scene.gltf");
                assert_eq!(backend, "scanline");
            }
            other => panic!("DAG was mutated despite factory error: {other:?}"),
        }
    }

    #[test]
    fn render3d_with_factory_rewrites_to_frame_source_pump() {
        // A stub factory is installed: it must be invoked with the
        // exact (source, backend, opts) triple from the DAG node, the
        // returned `FrameSource` must wind up wrapped in a
        // `SourcePump::Frames`, and the DAG node must be rewritten to
        // `FrameSource { source }` so downstream resolution treats it
        // as a normal frame leaf.
        use std::sync::Arc;
        use std::sync::Mutex;
        let captured: Arc<Mutex<Option<(String, String, serde_json::Value)>>> =
            Arc::new(Mutex::new(None));
        let captured_c = captured.clone();
        let factory: RenderSourceFactory = Box::new(move |s, b, o| {
            *captured_c.lock().unwrap() = Some((s.to_string(), b.to_string(), o.clone()));
            Ok(Box::new(StubFrameSource::new()) as Box<dyn FrameSource>)
        });

        let job = Job::from_json(r#"{"out.wav": {"audio": [{"from": "in.wav"}]}}"#).unwrap();
        let ctx = RuntimeContext::new();
        let executor = Executor::new(&job, &ctx).with_render_source_factory(factory);
        let mut dag = dag_with_render3d("scene.gltf", "scanline");
        let opened = executor
            .resolve_source_shapes(&mut dag)
            .expect("resolve should succeed with factory installed");

        // (a) factory invoked exactly once with the verbatim triple
        let cap = captured.lock().unwrap().clone();
        let (s, b, o) = cap.expect("factory was not called");
        assert_eq!(s, "scene.gltf");
        assert_eq!(b, "scanline");
        assert_eq!(o["width"], 4);
        assert_eq!(o["height"], 4);

        // (b) opened map keyed by source URI, value is Frames pump with
        // a synthesised single-stream descriptor
        assert_eq!(opened.len(), 1);
        let pump = opened.get("scene.gltf").expect("pump for source uri");
        match pump {
            SourcePump::Frames { streams, .. } => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].index, 0);
                assert_eq!(streams[0].params.codec_id.as_str(), "pcm_s16le");
            }
            SourcePump::Demuxer(_) => panic!("expected SourcePump::Frames, got Demuxer"),
            SourcePump::Packets(_) => panic!("expected SourcePump::Frames, got Packets"),
        }

        // (c) DAG node rewritten in place to FrameSource { source }
        match dag.node(NodeId(0)) {
            DagNode::FrameSource { source } => assert_eq!(source, "scene.gltf"),
            other => panic!("expected FrameSource after resolve, got {other:?}"),
        }
    }
}
