//! [`RemoteClient`] keyed propose/query against a multi-Raft [`CraftCluster`].

use std::sync::Arc;
use std::time::Duration;

use craft::CraftCluster;
use craft::NodeId;
use craft::core::{RaftGroupId, StableShardRouter, place_shard};
use craft::net::LocalNetwork;
use craft_client::{RemoteClient, TypedClient};
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, await_craft_leader,
    fast_raft_config_with_seed, find_keys_for_two_groups, wait_for_each_group_cluster_leader,
};

async fn spawn_multi_raft_cluster() -> (LocalNetwork, Vec<Arc<CraftCluster<KvMachine>>>) {
    let ids = [craft::NodeId(1), craft::NodeId(2), craft::NodeId(3)];
    let net = LocalNetwork::new();
    let shard_count = 64;
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(3))
            .tick_period(TICK_PERIOD)
            .shard_count(shard_count)
            .group_replication_factor(64)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    (net, clusters)
}

#[tokio::test(start_paused = true)]
async fn remote_client_routes_keyed_writes_and_reads_by_shard() {
    let (net, clusters) = spawn_multi_raft_cluster().await;
    let group_count = clusters.first().map(|c| c.raft_groups()).unwrap_or(0);
    wait_for_each_group_cluster_leader(&clusters, group_count).await;

    let shard_count = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(shard_count, &groups);

    let router = StableShardRouter::new(shard_count);
    let shard_a = router.shard_for(&route_a).expect("route_a active");
    let shard_b = router.shard_for(&route_b).expect("route_b active");
    assert_eq!(place_shard(shard_a, &groups), Some(groups[0]));
    assert_eq!(place_shard(shard_b, &groups), Some(groups[1]));

    let remote = RemoteClient::new(Arc::new(net.clone()), [NodeId(1)]);
    let client: TypedClient<RemoteClient, KvMachine> = TypedClient::new(remote);

    client
        .propose_keyed(
            &route_a,
            &KvCommand::Set {
                key: "k".into(),
                value: "g0".into(),
            },
        )
        .await
        .expect("propose group 0");

    client
        .propose_keyed(
            &route_b,
            &KvCommand::Set {
                key: "k".into(),
                value: "g1".into(),
            },
        )
        .await
        .expect("propose group 1");

    let got_a = client
        .query_keyed(&route_a, &KvQuery::Get { key: "k".into() })
        .await
        .expect("query group 0");
    assert_eq!(got_a, KvResponse::Value(Some("g0".into())));

    let got_b = client
        .query_keyed(&route_b, &KvQuery::Get { key: "k".into() })
        .await
        .expect("query group 1");
    assert_eq!(got_b, KvResponse::Value(Some("g1".into())));

    for c in clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn remote_client_keyed_read_on_follower_is_linearizable() {
    let (net, clusters) = spawn_multi_raft_cluster().await;
    let group_count = clusters.first().map(|c| c.raft_groups()).unwrap_or(0);
    wait_for_each_group_cluster_leader(&clusters, group_count).await;

    let leader = await_craft_leader(&clusters).await;
    let follower = clusters
        .iter()
        .find(|c| c.node_id() != leader.node_id())
        .expect("follower")
        .node_id();

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, _) = find_keys_for_two_groups(64, &groups);

    let write_client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    let write_typed: TypedClient<RemoteClient, KvMachine> = TypedClient::new(write_client);
    write_typed
        .propose_keyed(
            &route_a,
            &KvCommand::Set {
                key: "k".into(),
                value: "via-client".into(),
            },
        )
        .await
        .expect("seed write");

    let read_client = RemoteClient::new(Arc::new(net.clone()), [follower]).with_retry(
        craft_client::RetryPolicy {
            max_attempts: 5,
            attempt_timeout: Duration::from_secs(2),
            backoff: Duration::ZERO,
        },
    );
    let read_typed: TypedClient<RemoteClient, KvMachine> = TypedClient::new(read_client);
    let got = read_typed
        .query_keyed(&route_a, &KvQuery::Get { key: "k".into() })
        .await
        .expect("follower keyed read via client");
    assert_eq!(got, KvResponse::Value(Some("via-client".into())));

    for c in clusters {
        c.shutdown();
    }
}
