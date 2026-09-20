//! Pipeline composition + JSON-driven job graphs.
//!
//! Two layers of API live in this crate:
//!
//! * **Low-level** — [`Pipeline`] routes source streams to per-stream
//!   [`Sink`]s, deciding per-route whether to copy packets verbatim or
//!   transcode (decode + optional re-encode). Plus the [`remux`] /
//!   [`transcode_simple`] / [`transcode`] helpers.
//! * **High-level** — [`Job`] + [`Executor`]: a JSON-described
//!   transcode graph with aliases, filters, stream selectors, and
//!   either a single-threaded or stage-per-thread executor. Outputs
//!   plug in via [`JobSink`].

pub mod bench;
pub mod dag;
pub mod executor;
pub mod failure;
pub mod schema;
pub mod selection;
pub mod sinks;
pub mod staged;
pub mod validate;

pub use dag::{Dag, DagNode, NodeId};
pub use executor::{
    Executor, ExecutorHandle, JobSink, PipelineSourceShape, PipelineStageInfo, PipelineTopology,
    PipelineTrackInfo, RenderSourceFactory, TrackSink, TrackSinkInfo,
};
pub use failure::{FailureStage, RunFailure};
pub use oxideav_core::{FilterFactory, FilterRegistry};
pub use schema::{
    parse_pixel_format, ConvertNode, FilterNode, Job, OutputSpec, Render3DNode, SourceRef,
    StreamSelector, TrackInput, TrackSpec,
};
pub use selection::{
    make_decoder, make_decoder_with, make_decoder_with_selection, make_encoder, make_encoder_with,
    make_encoder_with_selection, CodecPreferences,
};
pub use sinks::{FileSink, NullSink};
pub use staged::{BarrierKind, ChannelCaps, Progress, SeekCmd};

use oxideav_core::{
    CodecParameters, CodecRegistry, Decoder, Demuxer, Encoder, Error, Frame, FrameLease, MediaType,
    Muxer, Packet, Result, StreamInfo, TimeBase,
};

// ───────────────────────── Sink trait ─────────────────────────

/// What a [`Sink`] wants for a given source stream.
///
/// `Transcode` carries a full [`CodecParameters`] (~200 B) which is
/// larger than the other variants; boxing the field would be an
/// API-visible churn and the enum is only constructed once per
/// stream/route resolution, so it's not on a hot path.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum SinkAcceptance {
    /// Take raw packets as-is — stream copy, no decoding.
    Copy,
    /// Want decoded frames that match `target` parameters. If the
    /// source already matches the target's core params, the pipeline
    /// degenerates this to a Copy route automatically.
    Transcode { target: CodecParameters },
    /// Not interested in this stream.
    Reject,
}

/// A per-stream output destination.
///
/// Sinks declare what they accept via [`accepts`](Sink::accepts), then
/// receive either raw packets (copy path) or decoded frames (transcode
/// path) depending on how the pipeline resolves the route.
pub trait Sink: Send {
    /// Inspect the source stream's parameters and declare interest.
    fn accepts(&self, src_params: &CodecParameters) -> SinkAcceptance;

    /// Copy path: receive a raw encoded packet.
    fn write_packet(&mut self, pkt: &Packet) -> Result<()>;

    /// Decoded/transcoded path: receive a decoded frame.
    fn write_frame(&mut self, frame: &Frame) -> Result<()>;

    /// Receive an owned decoded-frame lease without copying its backing
    /// storage. The default adapter materialises only for legacy sinks that
    /// implement [`Self::write_frame`] alone.
    fn write_frame_lease(&mut self, lease: FrameLease) -> Result<()> {
        let frame = lease.into_frame()?;
        self.write_frame(&frame)
    }

    /// Called once after all data has been sent through this sink.
    fn flush(&mut self) -> Result<()>;
}

// ───────────────────────── Pipeline ──────────────────────────

/// Processing mode resolved for one source→sink binding.
enum RouteMode {
    Copy,
    Decode {
        decoder: Box<dyn Decoder>,
    },
    Transcode {
        decoder: Box<dyn Decoder>,
        encoder: Box<dyn Encoder>,
    },
}

struct Route {
    source_stream: u32,
    sink: Box<dyn Sink>,
    mode: RouteMode,
}

/// Multi-stream pipeline: source demuxer → per-stream sinks.
pub struct Pipeline {
    source: Box<dyn Demuxer>,
    routes: Vec<Route>,
    codecs: CodecRegistry,
    prepared: bool,
    /// Forwarded to `make_decoder_with` / `make_encoder_with` at route
    /// resolution time. Default = no filtering. Set via
    /// [`Pipeline::with_preferences`] to e.g. opt out of hardware impls
    /// (`CodecPreferences { no_hardware: true, .. }`) without removing
    /// them from the registry.
    prefs: CodecPreferences,
}

/// Stats returned by [`Pipeline::run`].
#[derive(Clone, Copy, Debug, Default)]
pub struct PipelineStats {
    pub packets_read: u64,
    pub packets_copied: u64,
    pub frames_decoded: u64,
    pub packets_encoded: u64,
    pub packets_dropped: u64,
}

impl Pipeline {
    /// Create a pipeline from a demuxer and codec registry.
    pub fn new(source: Box<dyn Demuxer>, codecs: CodecRegistry) -> Self {
        Self {
            source,
            routes: Vec::new(),
            codecs,
            prepared: false,
            prefs: CodecPreferences::default(),
        }
    }

    /// Set the codec-resolution preferences (priority bias, hardware
    /// opt-out, name allow/exclude). Forwarded to `make_decoder_with` /
    /// `make_encoder_with` for every route. Builder-style — chain on
    /// `Pipeline::new(...)`.
    pub fn with_preferences(mut self, prefs: CodecPreferences) -> Self {
        self.prefs = prefs;
        self
    }

    /// Inspect the source container's streams before binding sinks.
    pub fn streams(&self) -> &[StreamInfo] {
        self.source.streams()
    }

    /// Bind a sink to a specific source stream by index.
    pub fn bind(&mut self, stream_index: u32, sink: Box<dyn Sink>) -> Result<()> {
        if self.prepared {
            return Err(Error::other("pipeline already prepared"));
        }
        let stream = self
            .source
            .streams()
            .iter()
            .find(|s| s.index == stream_index)
            .ok_or_else(|| Error::invalid(format!("no stream with index {stream_index}")))?;
        let acceptance = sink.accepts(&stream.params);
        let mode = self.resolve_mode(&stream.params, &acceptance)?;
        self.routes.push(Route {
            source_stream: stream_index,
            sink,
            mode,
        });
        Ok(())
    }

    /// Bind a sink to the first source stream matching a media type.
    pub fn bind_first(&mut self, media_type: MediaType, sink: Box<dyn Sink>) -> Result<()> {
        let idx = self
            .source
            .streams()
            .iter()
            .find(|s| s.params.media_type == media_type)
            .map(|s| s.index)
            .ok_or_else(|| Error::invalid(format!("no {media_type:?} stream in source")))?;
        self.bind(idx, sink)
    }

    /// Finalize routing: tell the demuxer which streams are active.
    pub fn prepare(&mut self) -> Result<()> {
        if self.prepared {
            return Ok(());
        }
        let active: Vec<u32> = self.routes.iter().map(|r| r.source_stream).collect();
        self.source.set_active_streams(&active);
        self.prepared = true;
        Ok(())
    }

    /// Run the pipeline to completion.
    pub fn run(&mut self) -> Result<PipelineStats> {
        if !self.prepared {
            self.prepare()?;
        }
        let mut stats = PipelineStats::default();
        loop {
            let pkt = match self.source.next_packet() {
                Ok(p) => p,
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            };
            stats.packets_read += 1;
            let route = match self
                .routes
                .iter_mut()
                .find(|r| r.source_stream == pkt.stream_index)
            {
                Some(r) => r,
                None => {
                    stats.packets_dropped += 1;
                    continue;
                }
            };
            match &mut route.mode {
                RouteMode::Copy => {
                    route.sink.write_packet(&pkt)?;
                    stats.packets_copied += 1;
                }
                RouteMode::Decode { decoder } => {
                    decoder.send_packet(&pkt)?;
                    drain_decoder_to_sink(&mut **decoder, &mut *route.sink, &mut stats)?;
                }
                RouteMode::Transcode { decoder, encoder } => {
                    decoder.send_packet(&pkt)?;
                    drain_transcode_to_sink(
                        &mut **decoder,
                        &mut **encoder,
                        &mut *route.sink,
                        &mut stats,
                    )?;
                }
            }
        }
        // Flush all routes.
        for route in &mut self.routes {
            match &mut route.mode {
                RouteMode::Copy => {}
                RouteMode::Decode { decoder } => {
                    decoder.flush()?;
                    drain_decoder_to_sink(&mut **decoder, &mut *route.sink, &mut stats)?;
                }
                RouteMode::Transcode { decoder, encoder } => {
                    decoder.flush()?;
                    drain_transcode_to_sink(
                        &mut **decoder,
                        &mut **encoder,
                        &mut *route.sink,
                        &mut stats,
                    )?;
                    encoder.flush()?;
                    drain_encoder_to_sink(&mut **encoder, &mut *route.sink, &mut stats)?;
                }
            }
            route.sink.flush()?;
        }
        Ok(stats)
    }

    fn resolve_mode(
        &self,
        src_params: &CodecParameters,
        acceptance: &SinkAcceptance,
    ) -> Result<RouteMode> {
        match acceptance {
            SinkAcceptance::Reject => Err(Error::other("sink rejected stream")),
            SinkAcceptance::Copy => Ok(RouteMode::Copy),
            SinkAcceptance::Transcode { target } => {
                if src_params.matches_core(target) {
                    Ok(RouteMode::Copy)
                } else if target.codec_id == src_params.codec_id
                    && target.media_type == src_params.media_type
                {
                    // Same codec but different params — decode only, let sink
                    // handle the format difference (e.g. resample).
                    let decoder =
                        selection::make_decoder_with(&self.codecs, src_params, &self.prefs)?;
                    Ok(RouteMode::Decode { decoder })
                } else {
                    let decoder =
                        selection::make_decoder_with(&self.codecs, src_params, &self.prefs)?;
                    let encoder = selection::make_encoder_with(&self.codecs, target, &self.prefs)?;
                    Ok(RouteMode::Transcode { decoder, encoder })
                }
            }
        }
    }
}

fn drain_decoder_to_sink(
    decoder: &mut dyn Decoder,
    sink: &mut dyn Sink,
    stats: &mut PipelineStats,
) -> Result<()> {
    loop {
        match decoder.receive_frame_lease() {
            Ok(frame) => {
                stats.frames_decoded += 1;
                sink.write_frame_lease(frame)?;
            }
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

fn drain_transcode_to_sink(
    decoder: &mut dyn Decoder,
    encoder: &mut dyn Encoder,
    sink: &mut dyn Sink,
    stats: &mut PipelineStats,
) -> Result<()> {
    loop {
        match decoder.receive_frame_lease() {
            Ok(lease) => {
                stats.frames_decoded += 1;
                let frame = lease.into_frame()?;
                encoder.send_frame(&frame)?;
                drain_encoder_to_sink(encoder, sink, stats)?;
            }
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

fn drain_encoder_to_sink(
    encoder: &mut dyn Encoder,
    sink: &mut dyn Sink,
    stats: &mut PipelineStats,
) -> Result<()> {
    loop {
        match encoder.receive_packet() {
            Ok(pkt) => {
                stats.packets_encoded += 1;
                sink.write_packet(&pkt)?;
            }
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

// ─────────────────── Legacy helpers (preserved) ──────────────

/// Copy all packets from `demuxer` into `muxer` without re-encoding.
pub fn remux(demuxer: &mut dyn Demuxer, muxer: &mut dyn Muxer) -> Result<u64> {
    muxer.write_header()?;
    let mut packets = 0u64;
    loop {
        match demuxer.next_packet() {
            Ok(pkt) => {
                muxer.write_packet(&pkt)?;
                packets += 1;
            }
            Err(Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    muxer.write_trailer()?;
    Ok(packets)
}

/// Plan describing how to derive an output stream from one input stream.
#[derive(Clone, Debug)]
pub enum StreamPlan {
    /// Stream-copy (codec passthrough, no decode).
    Copy,
    /// Decode then re-encode using the named codec. The encoder is constructed
    /// with parameters carried over from the input (sample rate, channels…)
    /// and the chosen codec id.
    Reencode { output_codec: String },
    /// Drop this stream — it appears in the input but should not appear in
    /// the output container. Useful when an input has e.g. `[video, audio]`
    /// and the caller only wants the audio.
    Drop,
}

/// Multi-stream transcode helper.
///
/// Reads from a multi-stream demuxer, applies a per-stream [`StreamPlan`]
/// chosen by `plan_for`, and muxes the resulting (copied or re-encoded)
/// packets into a single output container.
///
/// `plan_for` is called once per input stream during set-up, with the
/// stream's [`StreamInfo`]. Returning [`StreamPlan::Drop`] excludes that
/// stream from the output entirely. For the common single-stream case
/// pass `|_| Ok(StreamPlan::Reencode { output_codec: "...".into() })` or
/// `|_| Ok(StreamPlan::Copy)`.
///
/// `muxer_open` receives the **output** stream descriptors (one per
/// non-Drop input stream, with sequential `index = 0..N`) and returns
/// the muxer. For copy routes the output stream mirrors the input; for
/// reencode routes it carries the encoder's `output_params()` plus a
/// time base derived from the encoder's sample rate (audio) or the
/// input's time base (video / fallback).
///
/// Packet ordering is preserved end-to-end: input packets are read in
/// demuxer order and copy-route packets are forwarded immediately, so
/// interleave matches the source. Reencode-route packets emerge from
/// the encoder in decode order which the muxer is responsible for
/// interleaving (most container muxers handle this internally via pts).
pub fn transcode_simple(
    demuxer: &mut dyn Demuxer,
    muxer_open: impl FnOnce(&[StreamInfo]) -> Result<Box<dyn Muxer>>,
    codecs: &CodecRegistry,
    plan_for: impl Fn(&StreamInfo) -> Result<StreamPlan>,
) -> Result<TranscodeStats> {
    transcode_simple_with(
        demuxer,
        muxer_open,
        codecs,
        &CodecPreferences::default(),
        plan_for,
    )
}

/// Same as [`transcode_simple`] but threads `prefs` through to
/// `make_decoder_with` / `make_encoder_with`. Use this from CLIs that
/// expose a `--no-hwaccel` style flag or any codec-priority bias.
pub fn transcode_simple_with(
    demuxer: &mut dyn Demuxer,
    muxer_open: impl FnOnce(&[StreamInfo]) -> Result<Box<dyn Muxer>>,
    codecs: &CodecRegistry,
    prefs: &CodecPreferences,
    plan_for: impl Fn(&StreamInfo) -> Result<StreamPlan>,
) -> Result<TranscodeStats> {
    let in_streams = demuxer.streams().to_vec();
    if in_streams.is_empty() {
        return Err(Error::invalid("no streams in input"));
    }

    // Build per-stream routes. `route_by_in_idx` maps the input stream
    // index → route slot index (or `None` for Drop). The routes list
    // is densely numbered 0..N matching the output stream indices.
    let mut routes: Vec<TranscodeRoute> = Vec::with_capacity(in_streams.len());
    let mut out_streams: Vec<StreamInfo> = Vec::with_capacity(in_streams.len());
    let mut route_by_in_idx: std::collections::HashMap<u32, usize> =
        std::collections::HashMap::with_capacity(in_streams.len());

    for in_stream in &in_streams {
        let plan = plan_for(in_stream)?;
        match plan {
            StreamPlan::Drop => continue,
            StreamPlan::Copy => {
                let out_index = out_streams.len() as u32;
                let mut out = in_stream.clone();
                out.index = out_index;
                out_streams.push(out);
                route_by_in_idx.insert(in_stream.index, routes.len());
                routes.push(TranscodeRoute::Copy { out_index });
            }
            StreamPlan::Reencode { output_codec } => {
                let decoder = selection::make_decoder_with(codecs, &in_stream.params, prefs)?;
                let enc_params = build_encoder_params(&output_codec, in_stream)?;
                let encoder = selection::make_encoder_with(codecs, &enc_params, prefs)?;
                let out_params = encoder.output_params().clone();
                let out_index = out_streams.len() as u32;
                let out_time_base = match out_params.media_type {
                    MediaType::Audio => match out_params.sample_rate {
                        Some(sr) if sr > 0 => TimeBase::new(1, sr as i64),
                        _ => in_stream.time_base,
                    },
                    _ => in_stream.time_base,
                };
                out_streams.push(StreamInfo {
                    index: out_index,
                    time_base: out_time_base,
                    duration: in_stream.duration,
                    start_time: Some(0),
                    params: out_params,
                });
                route_by_in_idx.insert(in_stream.index, routes.len());
                routes.push(TranscodeRoute::Reencode {
                    out_index,
                    decoder,
                    encoder,
                });
            }
        }
    }

    if out_streams.is_empty() {
        return Err(Error::invalid(
            "every input stream was dropped — nothing to mux",
        ));
    }

    let mut muxer = muxer_open(&out_streams)?;
    muxer.write_header()?;

    let mut stats = TranscodeStats::default();

    // Pump packets from the demuxer in source order, dispatching by
    // input stream index. EOF triggers a flush of every reencode route.
    loop {
        match demuxer.next_packet() {
            Ok(pkt) => {
                stats.packets_in += 1;
                let Some(&slot) = route_by_in_idx.get(&pkt.stream_index) else {
                    // Input stream that the caller dropped (or that the
                    // demuxer surfaces but has no metadata for). Skip.
                    continue;
                };
                process_packet(&mut routes[slot], &pkt, &mut *muxer, &mut stats)?;
            }
            Err(Error::Eof) => {
                // Flush every reencode route, then write the trailer.
                for route in &mut routes {
                    if let TranscodeRoute::Reencode {
                        out_index,
                        decoder,
                        encoder,
                        ..
                    } = route
                    {
                        decoder.flush()?;
                        loop {
                            match decoder.receive_frame() {
                                Ok(frame) => {
                                    stats.frames_decoded += 1;
                                    encoder.send_frame(&frame)?;
                                    drain_encoder(
                                        &mut **encoder,
                                        *out_index,
                                        &mut *muxer,
                                        &mut stats,
                                    )?;
                                }
                                Err(Error::NeedMore) | Err(Error::Eof) => break,
                                Err(e) => return Err(e),
                            }
                        }
                        encoder.flush()?;
                        drain_encoder(&mut **encoder, *out_index, &mut *muxer, &mut stats)?;
                    }
                }
                break;
            }
            Err(e) => return Err(e),
        }
    }

    muxer.write_trailer()?;
    Ok(stats)
}

/// Build encoder params for an input stream + requested output codec id.
/// Carries the input's media-type-relevant parameters across so the
/// encoder constructor sees a fully-populated descriptor.
fn build_encoder_params(output_codec: &str, in_stream: &StreamInfo) -> Result<CodecParameters> {
    let codec_id: oxideav_core::CodecId = output_codec.into();
    match in_stream.params.media_type {
        MediaType::Audio => {
            let mut p = CodecParameters::audio(codec_id);
            p.sample_rate = in_stream.params.sample_rate;
            p.channels = in_stream.params.channels;
            p.sample_format = in_stream.params.sample_format;
            p.channel_layout = in_stream.params.channel_layout;
            Ok(p)
        }
        MediaType::Video => {
            let mut p = CodecParameters::video(codec_id);
            p.width = in_stream.params.width;
            p.height = in_stream.params.height;
            p.pixel_format = in_stream.params.pixel_format;
            p.frame_rate = in_stream.params.frame_rate;
            Ok(p)
        }
        MediaType::Subtitle => Ok(CodecParameters::subtitle(codec_id)),
        MediaType::Data | MediaType::Unknown => Err(Error::unsupported(format!(
            "transcode_simple Reencode: cannot reencode {:?} stream {}",
            in_stream.params.media_type, in_stream.index
        ))),
    }
}

enum TranscodeRoute {
    Copy {
        out_index: u32,
    },
    Reencode {
        out_index: u32,
        decoder: Box<dyn Decoder>,
        encoder: Box<dyn Encoder>,
    },
}

fn process_packet(
    route: &mut TranscodeRoute,
    pkt: &Packet,
    muxer: &mut dyn Muxer,
    stats: &mut TranscodeStats,
) -> Result<()> {
    match route {
        TranscodeRoute::Copy { out_index } => {
            // Copy path: rewrite stream_index to the muxer's view and
            // forward the packet verbatim.
            let mut out_pkt = pkt.clone();
            out_pkt.stream_index = *out_index;
            muxer.write_packet(&out_pkt)?;
            stats.packets_out += 1;
        }
        TranscodeRoute::Reencode {
            out_index,
            decoder,
            encoder,
        } => {
            decoder.send_packet(pkt)?;
            loop {
                match decoder.receive_frame() {
                    Ok(frame) => {
                        stats.frames_decoded += 1;
                        encoder.send_frame(&frame)?;
                        drain_encoder(&mut **encoder, *out_index, muxer, stats)?;
                    }
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Ok(())
}

fn drain_encoder(
    encoder: &mut dyn oxideav_core::Encoder,
    out_index: u32,
    muxer: &mut dyn Muxer,
    stats: &mut TranscodeStats,
) -> Result<()> {
    loop {
        match encoder.receive_packet() {
            Ok(mut pkt) => {
                // Encoders emit packets with stream_index = 0 (they only
                // know about a single output stream). Patch in the
                // muxer-side index so multi-stream outputs route correctly.
                pkt.stream_index = out_index;
                muxer.write_packet(&pkt)?;
                stats.packets_out += 1;
            }
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TranscodeStats {
    pub packets_in: u64,
    pub packets_out: u64,
    pub frames_decoded: u64,
}

// Compatibility re-export: keep the old `transcode` symbol for now (returns Unsupported).
#[deprecated(note = "use transcode_simple instead")]
pub fn transcode(
    _demuxer: &mut dyn Demuxer,
    _muxer: &mut dyn Muxer,
    _codecs: &CodecRegistry,
    _plans: &[StreamPlan],
) -> Result<()> {
    Err(Error::unsupported("use transcode_simple"))
}
