//! [`TrembitaAppBuilder`] cluster wiring (node id, voters, leader tasks).

#![allow(clippy::large_futures)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::NodeId;
use crate::TrembitaApp;
use crate::TrembitaConfigure;
use crate::cluster::{EmptyStateMachine, TrembitaCluster};
use crate::integration::boot_local_app;
use trembita_net::LocalNetwork;
use trembita_runtime::LeaderLoopOpts;
use trembita_test_support::{
    advance, eventually_default, fast_raft_config, wait_for_trembita_app_leader,
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
async fn empty_state_machine_cluster_re_elects_after_leader_shutdown() {
    let base = temp_base("reelect");
    let net = LocalNetwork::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];

    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = crate::builder::TrembitaClusterBuilder::new(id, EmptyStateMachine)
            .members(ids)
            .data_dir(base.join(format!("node-{}", id.0)))
            .raft_config(fast_raft_config())
            .tick_period(Duration::from_millis(5))
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20))
            .start_local(&net)
            .await;
        clusters.push(cluster);
    }

    for cluster in &clusters {
        assert_eq!(
            clusters
                .iter()
                .filter(|other| other.node_id() == cluster.node_id())
                .count(),
            1,
            "each node must have a distinct id"
        );
    }

    let mut leader_id = None;
    for _ in 0..500 {
        for cluster in &clusters {
            if cluster.is_leader().await {
                leader_id = Some(cluster.node_id());
                break;
            }
        }
        if leader_id.is_some() {
            break;
        }
        advance(Duration::from_millis(5)).await;
    }
    let leader_id = leader_id.expect("cluster should elect a leader");

    clusters
        .iter()
        .find(|c| c.node_id() == leader_id)
        .expect("leader cluster")
        .shutdown();
    clusters.retain(|c| c.node_id() != leader_id);

    for _ in 0..500 {
        for cluster in &clusters {
            if cluster.is_leader().await {
                cluster.shutdown();
                let _ = std::fs::remove_dir_all(&base);
                return;
            }
        }
        advance(Duration::from_millis(5)).await;
    }
    panic!("survivors failed to elect a new leader");
}
