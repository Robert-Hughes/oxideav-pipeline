mod common;

use oxideav_core::{MediaType, RuntimeContext};
use oxideav_pipeline::{ChannelCaps, Executor, Job, PipelineSourceShape, PipelineStageInfo};

#[test]
fn spawned_handle_reports_resolved_topology_and_caps() {
    let src = common::stub::touch("runtime-topology");

    let mut ctx = RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

    let job_json = format!(
        r#"{{
            "@in":   {{"all": [{{"from": "{}"}}]}},
            "@null": {{"audio": [{{"from": "@in", "copy": true}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let handle = Executor::new(&job, &ctx)
        .with_channel_caps(ChannelCaps {
            packets: 257,
            frames: 9,
        })
        .with_threads(2)
        .spawn()
        .expect("spawn executor");

    let topology = handle.pipeline_topology();
    assert_eq!(topology.output_name, "@null");
    assert_eq!(topology.packet_channel_capacity, 257);
    assert_eq!(topology.frame_channel_capacity, 9);
    assert_eq!(topology.tracks.len(), 1);

    let track = &topology.tracks[0];
    assert_eq!(track.source_shape, PipelineSourceShape::Demuxer);
    assert_eq!(track.media_type, MediaType::Audio);
    assert!(track.copy);
    assert!(matches!(track.stages.as_slice(), [PipelineStageInfo::Copy]));

    let depths = handle.pipeline_packet_queue_depths();
    assert_eq!(depths.len(), topology.tracks.len());
    assert!(depths
        .iter()
        .all(|depth| *depth <= topology.packet_channel_capacity));

    handle.stop().expect("stop executor");
}
