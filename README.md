# oxideav-pipeline

[![CI](https://github.com/OxideAV/oxideav-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/OxideAV/oxideav-pipeline/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/oxideav-pipeline.svg)](https://crates.io/crates/oxideav-pipeline) [![docs.rs](https://docs.rs/oxideav-pipeline/badge.svg)](https://docs.rs/oxideav-pipeline) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Pipeline composition for oxideav

Part of the [oxideav](https://github.com/OxideAV/oxideav-workspace) framework — a pure-Rust media transcoding and streaming stack. Codec, container, and filter crates are implemented from the spec (no C codec libraries linked or wrapped, no `*-sys` crates). Optional hardware-engine crates (`oxideav-videotoolbox` / `-audiotoolbox` / `-vaapi` / `-vdpau` / `-nvidia` / `-vulkan-video`) bridge to OS APIs via runtime `libloading`; pass `--no-hwaccel` (or omit the `hwaccel` feature) to opt out.

## Source shapes

The DAG executor branches on the [`SourceOutput`](https://docs.rs/oxideav-core)
returned by `SourceRegistry::open`:

- **Bytes** (file, http) — open container, demux, decode, encode, mux. The
  historical path; unchanged.
- **Packets** (rtmp, future srt/rtsp) — skip the container layer; pull packets
  directly from the source and feed the decoder.
- **Frames** (synthetic generators, future capture-card drivers) — skip both
  demux and decode; frames flow straight into the filter chain (or the sink
  if no filter is declared).

The shape is decided by the driver at registration time; jobs reference URIs
identically across the three.

All three shapes run on **both executor paths**. The pipelined
(stage-per-thread) runner spawns one source-pump thread per URI shaped
by the source kind: bytes-shape URIs get a demuxer thread, packet-shape
URIs get the same per-stream fan-out without the container layer, and
frame-shape URIs get a frame pump feeding the per-track frame-stage
chains directly (no demux, no decode stage). `Executor::spawn` — the
playback path with the live seek / progress / abort handle — therefore
works over typed sources too; a seek dispatched against a packet- or
frame-shape source (no seek surface on those traits) surfaces
`BarrierKind::SeekRejected` with the dispatch generation and the stream
keeps flowing. Serial/pipelined stats parity is pinned by
`tests/typed_source_staged.rs`.

## Graph validation

`Job::validate` walks every output/alias spec and rejects, with the
offending key in the message: empty specs, unresolved / reserved-sink
alias references, alias cycles (with the full path), empty `from`,
empty filter names, unknown pixel-format names, empty render3d
source/backend fields, explicitly-empty `codec` strings, and a
`stream_selector` whose `kind` contradicts the typed track list it
appears in. `Job::to_dag` additionally carries its own defensive
cycle guard, so calling it on an unvalidated cyclic job returns
`Err(InvalidData)` instead of overflowing the stack during alias
inlining (legal diamond re-use of one alias from several tracks still
resolves).

## `all:` fan-out over same-kind streams

An `all:` track expands into one runtime per source stream once the
source is open, each pinned to a `(kind, per-kind ordinal)` selector —
so a dual-audio source produces two distinct output tracks rather than
two copies of the first audio stream. Pinned by
`tests/all_tracks_fan_out_same_kind.rs` (serial + pipelined) via a
dual-stream stub demuxer with per-stream payload fill bytes.

## Error propagation

A failing node surfaces the ORIGINAL error from `Executor::run` on
both paths — `AbortState` keeps the first error, later cascade
symptoms are dropped, and the abort cascade wakes every blocked worker
(no hang). Serial delivers all pre-failure frames before surfacing the
error; the pipelined path deliberately trades in-flight frames
(bounded by the channel depths) for prompt teardown — the same
tradeoff `ExecutorHandle::stop` relies on. See
`tests/error_propagation.rs`.

## Failure attribution

`Executor::run_reporting` (and `ExecutorHandle::stop_reporting`)
return a `RunFailure` pairing that original error with *where* it
fired: the owning output key, a `FailureStage`
(`prepare` / `source` / `copy` / `decode` / `filter` / `convert` /
`encode` / `sink` / `sink-finish`), the track index when the failing
stage belongs to one, and the failing output's partial
`ExecutorStats` snapshot — "encoder failed on track 0 of out.mp4
after writing 132 frames" as data, not log-line archaeology. Both executor paths attribute the
same failure site to the same stage; `SinkFinish` is split from
`Sink` because the whole stream already landed when finalisation
fails (some engines treat that as salvageable). `run()` / `stop()`
stay thin wrappers: `From<RunFailure> for Error` recovers the stored
original error unwrapped, so error-KIND matching
(`ResourceExhausted`, …) behaves identically on both surfaces and the
first-error-wins contract is untouched. Job-level failures that
precede any per-output work (validation, DAG build) carry
`output: None`. See `tests/failure_attribution.rs`.

## Failed-output disposal

`Executor::with_discard_failed_outputs(true)` opts a job into
partial-output cleanup: a failing output's sink receives the
`JobSink::abandon` hook (default no-op) instead of being dropped
mid-write, and the built-in `FileSink` implements it by closing the
muxer's file handle and deleting the partially-written file — a
failed transcode leaves no half-file behind for a downstream consumer
to mistake for a finished output. The hook also fires for sinks
resolved in a multi-output wave whose preparation failed before the
wave started (their just-created zero-byte files would otherwise
linger). Healthy wave-mates of a failing output still finalise
normally, and a clean `ExecutorHandle::stop` (no recorded error)
still finalises via `finish` — stop is a cancel, not a failure. The
default (`false`) keeps the historical partial-file-stays behaviour,
which is the debugging-friendly choice. See
`tests/discard_failed_outputs.rs`.

## Multi-output parallelism

A job with several outputs and `threads ≥ 2` runs its outputs
**concurrently**: outputs are chunked into document-order waves whose
width is `ExecutionContext::effective_workers(n_outputs)` — the same
budget clamp codecs use for internal fan-out — and each wave's outputs
run on scoped threads, each granted an even split of the job budget as
its codec-internal `ExecutionContext`. Preparation (source opens,
codec instantiation, sink resolution) stays sequential in document
order, so setup errors keep their precedence and never interleave. If
an output fails, `Executor::run` surfaces the error of the *earliest
failing output in document order* — deterministic under any thread
timing; healthy wave-mates still run to completion and deliver their
full streams, and waves after the failure never start (matching the
sequential contract that outputs after a failure don't run).
`threads == 1` keeps the historical strictly-sequential serial loop.
The budget itself resolves through the framework's single threading
authority: `with_threads(n)` > the job's `threads` key >
`ExecutionContext::auto()`. See `tests/multi_output_parallel.rs`.

## Benchmarks

`cargo bench -p oxideav-pipeline --bench graph` — criterion
micro-benchmarks over the graph-resolution cold path (`Job::from_json`
/ `validate` / `to_dag` on wide, deep, and alias-chained synthetic
jobs) plus `Dag::describe` and the `TrackInput::walk` / `leaf`
accessors. Pure in-memory jobs; no fixtures, no codecs.

## Per-packet decoder error tolerance

A decoder error on a single packet (e.g. an AAC frame with a recoverable
bit-stream glitch) is logged + skipped, not propagated as a fatal stream
failure. The next packet flows through normally. Same model the H.264
decoder uses internally for per-slice errors. This matches what real-world
media playback expects — a corrupt frame mid-stream should mean a single
skipped frame, not a wedged player. See `tests/decoder_error_tolerance.rs`
for the contract test.

## Seek-barrier payload

`BarrierKind::SeekFlush` carries the demuxer's actual `landed_pts` plus
the matching `time_base`, so consumers re-anchor at the precise landing
position (typically the largest keyframe ≤ requested target) instead of
guessing from the next packet's pts. Without the carried payload an
engine would have to wait for the first post-barrier audio frame and
re-anchor there — typically 50-200 ms off because video lands on a
keyframe (≤ target) while audio lands on the next packet (≥ target).
With it, position display reads the new position the instant the barrier
fires. See `tests/seek_flush_carries_landed_pts.rs`.

## Channel-depth budget

`Executor::with_channel_caps(ChannelCaps { packets, frames })` overrides
the per-track packet- and frame-channel depth for the pipelined runner.
Defaults are 16 packets / 8 frames — back-pressures a stalled consumer
before memory blows up, large enough to amortise the channel mutex cost
on each send.

Memory upper bound per output (rough):

```text
N_tracks * (packets * packet_size + frames * frame_size)
```

Tight (`{ packets: 1, frames: 1 }`) is the smallest legal depth — every
queue degenerates to one element so the source thread blocks until the
consumer has drained. Useful for embedded playback. Loosened
(`{ packets: 64, frames: 32 }`) lets bursty decoders coast on the queue
depth instead of blocking on the encoder — useful for high-throughput
offline transcodes. Zero is clamped up to one (`sync_channel(0)` is a
rendezvous channel and would serialise the entire pipeline). The serial
path uses no channels and ignores the cap. See
`tests/channel_caps.rs`.

## Memory-bounded packet queue

`Executor::with_max_queue_bytes(n)` adds an orthogonal *byte* ceiling on
the demuxer→worker packet queues. The channel-depth caps bound those
queues by element count — fine when packet sizes are uniform, but a
single outsized packet (a tracker module delivered whole in one packet,
a 4K intra keyframe, a JPEG-2000 codestream) can be megabytes on its
own, so the count cap alone lets resident memory swing widely with
content. The byte ceiling makes the demuxer park before reading the
next packet while the aggregate in-flight packet bytes are at or above
`n`; the consuming stage (copy or decode) frees the bytes the instant
it pulls the packet off the channel.

The two knobs compose — whichever binds first applies. A lone packet
larger than `n` is still admitted (the demuxer crosses the line by one
packet rather than deadlock on a packet bigger than the whole budget),
so `n` is a soft target, not a hard never-exceed. `0` (the default)
disables the ceiling entirely, leaving only the count caps; the serial
path ignores it. See `tests/max_queue_bytes.rs`.

## Back-pressure visibility

`ExecutorHandle::try_progress()` returns a `Progress { pts, frames, eof,
queue_bytes, elapsed_micros }`. `queue_bytes` reports the current
in-flight packet-byte total tracked by the `with_max_queue_bytes(n)`
budget — a value pinned to `n` indicates the demuxer is parking on the
byte ceiling waiting for the consumer to drain, a value hovering near
zero means the ceiling isn't binding (the count caps or downstream
block first). When no ceiling is configured the field is always `0`
(the budget short-circuits its accounting); at EOF the field returns
to `0` because every admitted packet has a matching release by the
time workers join. See `tests/progress_reports_queue_bytes.rs`.

## Wall-clock progress

`elapsed_micros` reports the monotonic wall-clock microseconds since
the pipelined runner's baseline `Instant`, captured just before the
first worker thread spawns. Engines use this to derive the realtime
ratio (`pts_micros / elapsed_micros`) without bracketing
`spawn()`/`stop()` with their own `Instant::now()`, to detect when a
live source is outpacing the pipeline (`pts < elapsed_micros` and
falling further behind every poll), and to print the EOF wall-clock
total from the `eof: true` progress event. The serial path
(`Executor::run`) doesn't wire a progress channel and never emits
`Progress`, so the field is only ever non-zero on the pipelined
runner reached via `Executor::spawn`. Values are guaranteed
non-decreasing across consecutive emissions because `Instant::elapsed`
is monotonic. See `tests/progress_reports_elapsed_micros.rs`.

## Copy-stage-progress visibility

`Progress::packets_copied` mirrors `ExecutorStats::packets_copied` but
is sampled on every `Progress` emission, so engines can distinguish
the copy and transcode sides of a mixed output without waiting for
EOF. A remux job whose audio track copies while the video track
transcodes will see `packets_copied` and `packets_encoded` advance
independently; a wedged copy stage shows up as a flat
`packets_copied` while `packets_read` keeps climbing (the demuxer is
still serving packets but they're not reaching the mux loop). The
field is monotonically non-decreasing, the EOF emission's value
matches the final `ExecutorStats::packets_copied`, and the field is
always `0` on transcode-only outputs (no track uses the copy path)
and on the serial path (no progress channel wired). See
`tests/progress_reports_packets_copied.rs`.

## Encoder-progress visibility

`Progress::packets_encoded` mirrors `ExecutorStats::packets_encoded`
but is sampled on every `Progress` emission, so engines can
distinguish a stalled encoder from a stalled decoder without waiting
for EOF. Because the field is live-sampled rather than only readable on
the final `Executor::stop()` snapshot, a CLI status bar can surface
"encoded N packets / decoded M frames" without instrumenting the
encoder externally, and the diagnostic signature of a wedged encoder is
`frames` and
`packets_read` both climbing while `packets_encoded` stays flat. The
field is monotonically non-decreasing, the EOF emission's value
matches the final `ExecutorStats::packets_encoded`, and the field is
always `0` on copy-only outputs (the staged runner skips
`run_encode_stage` when no encoder is named) and on the serial path
(no progress channel wired). See
`tests/progress_reports_packets_encoded.rs`.

## Demuxer-progress visibility

`Progress::packets_read` mirrors `ExecutorStats::packets_read` but is
sampled on every `Progress` emission, so engines can distinguish a
stalled decoder from a stalled source without waiting for EOF.
Headroom = `packets_read - frames - packets_skipped` is the count of
demuxed packets the decode stage hasn't yet resolved; pinned at the
channel-depth budget with a flat `frames` field is the diagnostic
signature of a wedged decoder. The field is monotonically
non-decreasing, the EOF emission's value matches the final
`ExecutorStats::packets_read`, and the serial path (no progress
channel wired) never populates it. See
`tests/progress_reports_packets_read.rs`.

## Decoder-skip visibility

`Progress::packets_skipped` and `ExecutorStats::packets_skipped`
expose the same cumulative count of packets the decoder swallowed
under the per-packet error-tolerance contract (see the
"Per-packet decoder error tolerance" section above). This gives an
engine a programmatic way to display "N decode errors" on its status
bar instead of reverse-engineering the skip count from
`packets_read - frames_decoded`.
Both decode paths (`staged::run_decode_stage` and the inherent
`executor::pump_packet`) increment a shared counter on every
`send_packet` error and on every `receive_frame` error that landed
before a frame was produced for that packet; a `produced_any` flag
prevents double-counting when a `receive_frame` error fires *after*
partial output already streamed (the packet wasn't lost — partial
output landed). The counter is monotonically non-decreasing, equals
`0` on a clean stream and on copy-only outputs (no decoder
instantiated), and the EOF `Progress` emission's value always
matches the final `ExecutorStats` value (both read from the same
atomic; EOF emission is sent after the worker threads join). See
`tests/progress_reports_packets_skipped.rs` plus the two new stats
assertions in `tests/decoder_error_tolerance.rs`.

## Seek correlation

`ExecutorHandle::seek_with_generation(stream_idx, pts, tb)` returns the
`generation: u32` the handle assigned to the dispatch; the resulting
`BarrierKind::SeekFlush { generation, .. }` / `SeekRejected { generation }`
carries the exact same value. Engines that need to ignore stale
pre-seek payloads (or detect a missed seek across a burst — holding
`→` during scrubbing fires several seeks per second) compare against
the returned value rather than maintaining a parallel mirror counter
that could silently desync with the demuxer's. The shorter
`seek(...) -> Result<()>` form is retained as a discard wrapper for
callers that don't need correlation. See
`tests/seek_with_generation.rs`.

## Multi-source seek fan-out

A job whose tracks come from several source URIs (separate audio +
video files, say) re-anchors ALL of them on one
`ExecutorHandle::seek`: the handle's receiver is owned by the first
routed source pump, which forwards every `SeekCmd` to a dedicated
channel per sibling routed source before handling it locally. Each
source answers with its own barrier — `SeekFlush` with its landed pts
(and the time base that pts is expressed in), or `SeekRejected` when
it has no seek surface — all stamped with the dispatch generation, so
every track observes exactly one barrier per generation. A command
addressing a stream a source doesn't route still seeks that source:
it retargets at its first routed stream with the pts rescaled into
that stream's time base, landing every source on the same presentation
instant. Matching stream indices also rescale: indices are local to each
source, so audio and video can both be stream zero with different time bases.
Historically only the first routed source received the seek
and the others silently kept their old position. See
`tests/seek_multi_source.rs` plus the `resolve_seek_target` unit
tests.

## TrackInput typed accessors

The recursive [`TrackInput`] node carries four variants — `Source`,
`Filter`, `Convert`, `Render3D` — and historically every consumer
had to re-`match` the enum to inspect a node. The schema now
exposes small typed primitives:

- `kind_str() -> &'static str` — stable discriminator for log lines
  + diagnostics (`"source"` / `"filter"` / `"convert"` / `"render3d"`).
- `is_source()` / `is_filter()` / `is_convert()` / `is_render3d()` —
  boolean predicates.
- `as_source()` / `as_filter()` / `as_convert()` / `as_render3d()` —
  borrowing accessors that return `Option<&Payload>` so callers can
  read inner fields (`from`, `filter`, `convert`, `source`, …)
  without re-matching.
- `upstream() -> Option<&TrackInput>` — single-step descent through
  the wrapper chain (`Filter` and `Convert` each carry exactly one
  upstream input today; `Source` and `Render3D` are leaves).
- `leaf() -> &TrackInput` — walk wrappers all the way to the
  terminal node in one call.
- `walk(|node| …)` — visitor that fires once per node from outermost
  to leaf, matching the order historical hand-rolled recursions
  use (see `validate::walk_input`).

Purely additive: every existing match-on-`TrackInput` site keeps
compiling. Consumer crates building lints, diagnostics, or DAG
transforms over a parsed `Job` lose the boilerplate.

## Usage

```toml
[dependencies]
oxideav-pipeline = "0.0"
```

## License

MIT — see [LICENSE](LICENSE).
