//! [`TrembitaAppBuilder`] cluster wiring (node id, voters, leader tasks).

#![allow(clippy::large_futures)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita::NodeId;
use trembita::RunOpts;
use trembita::TrembitaApp;
use trembita::TrembitaConfigure;
use trembita_net::LocalNetwork;
use trembita_runtime::LeaderLoopOpts;
use trembita_test_support::{
    advance, boot_local_app, eventually_default, fast_raft_config, wait_for_trembita_app_leader,
};

fn temp_base(label: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "trembita-app-cluster-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("tempdir");
    base
}

#[tokio::test(start_paused = true)]
async fn trembita_app_three_node_cluster_re_elects_after_leader_shutdown() {
    let base = temp_base("reelect");
    let net = LocalNetwork::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];

    let mut apps = Vec::new();
    for &id in &ids {
        let app = TrembitaApp::builder()
            .data_dir(base.join(format!("node-{}", id.0)))
            .configure(TrembitaConfigure {
                node_id: Some(id),
                raft_config: fast_raft_config(),
                tick_period: Duration::from_millis(5),
                reconcile_period: Duration::from_millis(20),
                directory_publish_period: Duration::from_millis(20),
                ..TrembitaConfigure::default()
            })
            .members(ids)
            .boot_for_test(RunOpts::local().with_local_net(net.clone()))
            .await
            .expect("boot");
        apps.push(app);
    }

    for app in &apps {
        assert_eq!(
            apps.iter()
                .filter(|other| other.node_id() == app.node_id())
                .count(),
            1,
            "each node must have a distinct id"
        );
    }

    let mut leader_id = None;
    for _ in 0..500 {
        for app in &apps {
            if app.is_leader().await {
                leader_id = Some(app.node_id());
                break;
            }
        }
        if leader_id.is_some() {
            break;
        }
        advance(Duration::from_millis(5)).await;
    }
    let leader_id = leader_id.expect("cluster should elect a leader");

    apps.iter()
        .find(|app| app.node_id() == leader_id)
        .expect("leader app")
        .shutdown();
    apps.retain(|app| app.node_id() != leader_id);

    for _ in 0..500 {
        for app in &apps {
            if app.is_leader().await {
                app.shutdown();
                let _ = std::fs::remove_dir_all(&base);
                return;
            }
        }
        advance(Duration::from_millis(5)).await;
    }
    panic!("survivors failed to elect a new leader");
}

#[tokio::test(start_paused = true)]
async fn trembita_app_on_leader_runs_on_product_builder() {
    let base = temp_base("on-leader");
    let data_dir = base.clone();
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_in_task = Arc::clone(&ticks);

    let app = boot_local_app(
        move || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
                .data_dir(data_dir)
                .on_leader(
                    LeaderLoopOpts::new(Duration::from_millis(10)).run_on_acquire(),
                    move |_| {
                        let ticks = Arc::clone(&ticks_in_task);
                        async move {
                            ticks.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    eventually_default("on_leader ticks", || ticks.load(Ordering::SeqCst) >= 2).await;

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
