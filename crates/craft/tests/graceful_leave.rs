//! Graceful cluster leave — mirrors the `craft-node` shutdown path when
//! `CRAFT_GRACEFUL_LEAVE=1`: call [`CraftCluster::leave`] before shutdown.

use std::sync::Arc;

use craft::CraftCluster;
use craft::net::LocalNetwork;
use craft::proto::NodeId;
use craft_test_support::{
    Kv, TICK_PERIOD, advance, await_craft_leader, eventually_async_default, fast_raft_config,
};

async fn voters_on_peers(clusters: &[Arc<CraftCluster<Kv>>], target: NodeId) -> Vec<Vec<NodeId>> {
    let mut out = Vec::new();
    for cluster in clusters {
        if cluster.node_id() == target {
            continue;
        }
        let voters = cluster.status().await.expect("peer status").voters;
        out.push(voters);
    }
    out
}

#[tokio::test(start_paused = true)]
async fn graceful_leave_removes_departing_node_before_shutdown() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(fast_raft_config())
            .tick_period(TICK_PERIOD)
            .allow_leave(true)
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    await_craft_leader(&clusters).await;

    let leaver = Arc::clone(&clusters[2]);
    let membership = leaver.leave().await.expect("graceful leave");
    assert!(
        !membership.voters.contains(&NodeId(3)),
        "leave response still lists departing node: {membership:?}"
    );

    eventually_async_default("peers drop departed voter", || async {
        let peer_views = voters_on_peers(&clusters, NodeId(3)).await;
        peer_views.len() == 2 && peer_views.iter().all(|voters| !voters.contains(&NodeId(3)))
    })
    .await;

    leaver.shutdown();
    for cluster in &clusters[0..2] {
        cluster.shutdown();
    }
    advance(TICK_PERIOD).await;
}
