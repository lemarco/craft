//! Leader-coordinated rolling upgrade over `LocalNetwork` (dry-run, no process exit).

use std::sync::Arc;
use std::time::Duration;

use crafty::upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeMachine, UpgradeOpts, UpgradeQuery, UpgradeResponse,
    spawn_upgrade_runtime,
};
use crafty::CraftyCluster;
use crafty::net::LocalNetwork;
use crafty::proto::NodeId;
use crafty_test_support::{
    TICK_PERIOD, advance, await_crafty_leader, eventually_async_default, fast_raft_config,
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
async fn coordinator_rolls_all_nodes_dry_run() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let tmp = TempDir::new().expect("tempdir");
    let bytes = b"crafty self-update demo artifact";
    let artifact_path = tmp.path().join("artifact.bin");
    std::fs::write(&artifact_path, bytes).expect("write artifact");
    let sha256_hex = hex::encode(Sha256::digest(bytes));

    let mut clusters = Vec::new();
    for &id in &ids {
        let install = tmp.path().join(format!("node-{id}"));
        let opts = UpgradeOpts {
            install_dir: install.clone(),
            current_link: install.join("current"),
            tick_period: Duration::from_millis(50),
            dry_run: true,
        };
        let cluster = Arc::new(
            CraftyCluster::builder(id, UpgradeMachine::default())
                .members(ids)
                .raft_config(fast_raft_config())
                .tick_period(TICK_PERIOD)
                .allow_leave(true)
                .start_local(&net)
                .await,
        );
        let _coordinator = spawn_upgrade_runtime(Arc::clone(&cluster), opts);
        clusters.push(cluster);
    }

    await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let leader = clusters
        .iter()
        .find(|c| c.is_leader().await)
        .expect("leader")
        .clone();

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

    eventually_async_default("fleet upgrade dry-run complete", || {
        let clusters = clusters.clone();
        async move {
            advance(Duration::from_millis(300)).await;
            let view = upgrade_view(&clusters[0]).await;
            view.fleet_ready && view.completed.len() == 3
        }
    })
    .await;

    for cluster in &clusters {
        cluster.shutdown();
    }
}
