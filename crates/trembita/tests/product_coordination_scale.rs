//! B-32 — product coordination scale (multi-Raft + sharded job queue via `TrembitaApp`).

#![allow(clippy::large_futures)]

use std::path::PathBuf;
use std::time::Duration;

use trembita::{AppManifest, JobOpts, QueueOpts, TrembitaApp, TrembitaConfigure, consumer};
use trembita_test_facade::{boot_local_app, wait_for_trembita_app_leader};
use trembita_test_support::advance;

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

#[consumer("jobs")]
async fn handle_jobs(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn trembita_app_sharded_job_queue_enqueues_on_logical_stream() {
    let base = temp_data_dir("b32-sharded");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().jobs([JobOpts::new("jobs")
                        .lease(Duration::from_secs(60))
                        .sharded(3)
                        .consumer(&HandleJobsConsumer)]),
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    assert!(app.job_queue("jobs").is_some());
    let id = app.enqueue("jobs", b"shard-me").await.expect("enqueue");
    assert!(id.0 >= 1);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn trembita_app_auto_shard_queue_accepts_enqueue() {
    let base = temp_data_dir("b32-auto");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new()
                        .queue([QueueOpts::new("burst", Duration::from_secs(30)).auto_shard()]),
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    app.enqueue("burst", b"auto").await.expect("enqueue");
    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn trembita_app_multi_raft_coordination_boots_with_empty_state_machine() {
    let base = temp_data_dir("b32-raft");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_raft_groups(2)
                        .with_coordination_shard_count(64)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("meta", Duration::from_secs(30))]),
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    assert_eq!(app.cluster().raft_groups(), 2);
    assert_eq!(app.cluster().shard_count(), 64);

    let added = app.add_raft_groups(1).await.expect("expand catalog");
    assert_eq!(added.len(), 1);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[cfg(feature = "dev-certs")]
mod from_config_tests {
    use super::*;
    use std::net::SocketAddr;

    use trembita_assembly::env_config::{AppConfig, EnvOverrides};
    use trembita_assembly::security::Security;
    use trembita_net::PeerDirectory;
    use trembita_proto::{JoinRole, NodeId};
    use trembita_runtime::DEFAULT_DRAIN_TIMEOUT;

    fn solo_app_config(data_dir: PathBuf, patch: impl FnOnce(&mut AppConfig)) -> AppConfig {
        let listen: SocketAddr = "127.0.0.1:19443".parse().expect("listen");
        let node_id = NodeId(1);
        let ca = trembita_net::tls::ClusterCa::generate().expect("ca");
        let security = Security::dev(&ca, node_id).expect("security");
        let mut peers = PeerDirectory::new();
        peers.insert(node_id, listen);
        let mut cfg = AppConfig {
            node_id,
            listen,
            http: None,
            http_tls: None,
            peers,
            members: vec![node_id],
            join_seeds: Vec::new(),
            allow_join: true,
            allow_voter_join: false,
            join_role: JoinRole::Learner,
            allow_leave: true,
            graceful_leave: true,
            voter_replacement: true,
            voter_replacement_grace_ticks: None,
            security,
            pem_paths: None,
            cert_dir: None,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            cert_watch: Duration::from_secs(60),
            data_dir: Some(data_dir),
            job_queue_stream: None,
            job_queue_lease: Duration::from_secs(60),
            job_queue_shards: None,
            job_queue_auto_shard: false,
            coordination_raft_groups: 1,
            coordination_shard_count: None,
            coordination_growth_profile: None,
            http_drain_timeout: trembita_assembly::DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            env: EnvOverrides::default(),
        };
        patch(&mut cfg);
        cfg
    }

    #[tokio::test(start_paused = true)]
    async fn from_config_env_only_sharded_job_queue_enqueues() {
        let base = temp_data_dir("b32-from-config-queue");
        let cfg = solo_app_config(base.clone(), |c| {
            c.job_queue_stream = Some("imports".into());
            c.job_queue_shards = Some(2);
        });

        let app = boot_local_app(
            || {
                TrembitaApp::from_config(cfg).configure(
                    TrembitaConfigure::default().with_tick_period(Duration::from_millis(5)),
                )
            },
            None,
        )
        .await;

        wait_for_trembita_app_leader(&app).await;
        advance(Duration::from_millis(200)).await;

        app.enqueue("imports", b"via-env-config")
            .await
            .expect("enqueue");
        app.shutdown();
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test(start_paused = true)]
    async fn from_config_multi_raft_groups_via_app_config() {
        let base = temp_data_dir("b32-from-config-raft");
        let cfg = solo_app_config(base.clone(), |c| {
            c.coordination_raft_groups = 3;
            c.coordination_shard_count = Some(32);
        });

        let app = boot_local_app(
            || {
                TrembitaApp::from_config(cfg).configure(
                    TrembitaConfigure::default().with_tick_period(Duration::from_millis(5)),
                )
            },
            None,
        )
        .await;

        wait_for_trembita_app_leader(&app).await;
        advance(Duration::from_millis(200)).await;

        assert_eq!(app.cluster().raft_groups(), 3);
        assert_eq!(app.cluster().shard_count(), 32);

        app.shutdown();
        let _ = std::fs::remove_dir_all(base);
    }
}
