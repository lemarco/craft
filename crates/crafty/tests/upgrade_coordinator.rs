//! Leader-coordinated rolling upgrade over `LocalNetwork` (dry-run, no process exit).

use std::sync::Arc;
use std::time::Duration;

use crafty::cluster::CraftyCluster;
use crafty::net::LocalNetwork;
use crafty::proto::NodeId;
use crafty::upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeMachine, UpgradeOpts, UpgradeQuery, UpgradeResponse,
    spawn_upgrade_coordinator,
};
use crafty_test_support::{
    TICK_PERIOD, advance, await_crafty_leader, eventually_async_default, fast_raft_config,
    wait_for_crafty_leader,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

async fn upgrade_view(cluster: &CraftyCluster<UpgradeMachine>) -> crafty::UpgradeView {
    let members = cluster.members().to_vec();
    let UpgradeResponse::View(view) = cluster
        .handle()
        .query(UpgradeQuery::View { members })
        .await
        .expect("query")
    else {
        panic!("expected view");
    };
    view
}

#[tokio::test(start_paused = true)]
async fn single_node_set_desired() {
    let net = LocalNetwork::new();
    let cluster = CraftyCluster::builder(NodeId(1), UpgradeMachine::default())
        .members([NodeId(1)])
        .raft_config(fast_raft_config())
        .tick_period(TICK_PERIOD)
        .start_local(&net)
        .await;
    wait_for_crafty_leader(&cluster).await;
    cluster
        .handle()
        .propose(UpgradeCommand::SetDesired(ArtifactManifest {
            app_version: "1.0.0".into(),
            url: "file:///tmp/x".into(),
            sha256_hex: "00".repeat(64),
            min_protocol: None,
        }))
        .await
        .expect("propose");
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn coordinator_rolls_all_nodes_dry_run() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let tmp = TempDir::new().expect("tempdir");
    let bytes = b"crafty self-update demo artifact";
    let artifact_path = tmp.path().join("artifact.bin");
    std::fs::write(&artifact_path, bytes).expect("write artifact");
    let sha256_hex = hex::encode(Sha256::digest(bytes));

    let mut clusters = Vec::new();
    let mut opts_list = Vec::new();
    for &id in &ids {
        let install = tmp.path().join(format!("node-{}", id.0));
        opts_list.push(UpgradeOpts {
            install_dir: install.clone(),
            current_link: install.join("current"),
            tick_period: Duration::from_millis(50),
            dry_run: true,
        });
        let cluster = Arc::new(
            CraftyCluster::builder(id, UpgradeMachine::default())
                .members(ids)
                .raft_config(fast_raft_config())
                .tick_period(TICK_PERIOD)
                .allow_leave(true)
                .start_local(&net)
                .await,
        );
        clusters.push(cluster);
    }

    let leader = await_crafty_leader(&clusters).await;
    advance(TICK_PERIOD).await;

    for (cluster, opts) in clusters.iter().zip(opts_list) {
        let _coordinator = spawn_upgrade_coordinator(Arc::clone(cluster), opts);
    }
    advance(Duration::from_millis(100)).await;

    leader
        .handle()
        .propose(UpgradeCommand::SetDesired(ArtifactManifest {
            app_version: "9.9.9".into(),
            url: format!("file://{}", artifact_path.display()),
            sha256_hex,
            min_protocol: None,
        }))
        .await
        .expect("set desired");

    let leader_for_poll = Arc::clone(&leader);
    eventually_async_default("fleet upgrade dry-run complete", move || {
        let leader = Arc::clone(&leader_for_poll);
        async move {
            for _ in 0..30 {
                advance(Duration::from_millis(100)).await;
            }
            let view = upgrade_view(leader.as_ref()).await;
            view.fleet_ready && view.completed.len() == 3
        }
    })
    .await;

    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn set_desired_replicates_on_three_node_cluster() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        clusters.push(Arc::new(
            CraftyCluster::builder(id, UpgradeMachine::default())
                .members(ids)
                .raft_config(fast_raft_config())
                .tick_period(TICK_PERIOD)
                .start_local(&net)
                .await,
        ));
    }
    let leader = await_crafty_leader(&clusters).await;
    wait_for_crafty_leader(leader.as_ref()).await;
    assert!(
        leader.handle().status().await.is_some(),
        "leader runtime stopped before propose"
    );

    leader
        .handle()
        .propose(UpgradeCommand::SetDesired(ArtifactManifest {
            app_version: "1.2.3".into(),
            url: "file:///tmp/x".into(),
            sha256_hex: "00".repeat(64),
            min_protocol: None,
        }))
        .await
        .expect("propose");

    advance(TICK_PERIOD).await;
    let view = upgrade_view(leader.as_ref()).await;
    assert_eq!(
        view.desired.as_ref().map(|d| d.app_version.as_str()),
        Some("1.2.3")
    );

    for cluster in &clusters {
        cluster.shutdown();
    }
}
