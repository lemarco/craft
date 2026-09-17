//! B-37 — coordination growth presets (configure + manifest queue auto-shard).

use std::path::PathBuf;
use std::time::Duration;

use trembita::{
    AppManifest, CoordinationGrowthPreset, QueueOpts, ReadyOpts, TrembitaApp, TrembitaConfigure,
};
use trembita_test_facade::boot_local_app;

fn temp_data_dir(prefix: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "trembita-{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("tempdir");
    base
}

#[tokio::test]
async fn b37_jobs_backlog_preset_applies_auto_shard_to_standard_queue() {
    let base = temp_data_dir("b37-jobs-backlog");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::JobsBacklog)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("imports", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let plan = app.scale_plan();
    assert_eq!(plan.job_queues.len(), 1);
    assert_eq!(plan.job_queues[0].mode, "auto_shard");
    assert_eq!(plan.coordination.coordination_raft_groups, 1);
    assert_eq!(app.cluster().raft_groups(), 1);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b37_write_sharding_preset_boots_multi_raft_without_auto_shard_queue() {
    let base = temp_data_dir("b37-write-sharding");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::WriteSharding)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("meta", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    assert_eq!(app.cluster().raft_groups(), 2);
    assert_eq!(app.cluster().shard_count(), 64);
    let plan = app.scale_plan();
    assert_eq!(plan.job_queues[0].mode, "standard");
    assert_eq!(plan.coordination.coordination_raft_groups, 2);
    assert_eq!(plan.coordination.coordination_shard_count, Some(64));

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b37_full_preset_boots_multi_raft_with_auto_shard_queue() {
    let base = temp_data_dir("b37-full");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::Full)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("jobs", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    assert_eq!(app.cluster().raft_groups(), 2);
    assert_eq!(app.cluster().shard_count(), 64);
    let plan = app.scale_plan();
    assert_eq!(plan.job_queues[0].mode, "auto_shard");
    assert_eq!(plan.coordination.coordination_raft_groups, 2);
    assert_eq!(plan.coordination.coordination_shard_count, Some(64));

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b37_standard_preset_keeps_single_raft_standard_queue() {
    let base = temp_data_dir("b37-standard");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::Standard)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("plain", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let plan = app.scale_plan();
    assert_eq!(plan.job_queues[0].mode, "standard");
    assert_eq!(plan.coordination.coordination_raft_groups, 1);
    assert_eq!(app.cluster().raft_groups(), 1);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
