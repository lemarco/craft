//! End-to-end client tests against a real 3-node cluster wired over the
//! in-memory `LocalNetwork` transport: the [`RemoteClient`]/[`TypedClient`]
//! drive live nodes through `craft_actor`'s `NodeService`, exercising
//! transparent follower→leader forwarding (client-routing) and failover/retry (F4).
//!
//! Retry-policy edge cases (`NoTargets`, timeout, `NotLeader`) live in
//! [`retry.rs`](retry.rs).

use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_core::RaftNode;
use craft_actor::craft_proto::NodeId;
use craft_actor::{NodeHandle, NodeService, RaftDriver, RuntimeConfig, spawn_node};
use craft_client::{Client, RemoteClient, RetryPolicy, TypedClient};
use craft_net::LocalNetwork;
use craft_test_support::{Cmd, Kv, Qry, Resp, TICK_PERIOD, await_node_leader, fast_raft_config};

fn spawn_cluster() -> (LocalNetwork, Vec<(NodeId, NodeHandle<Kv>)>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let transport: Arc<dyn craft_net::Transport> = Arc::new(net.clone());

    let mut handles = Vec::new();
    for &id in &ids {
        let node = RaftNode::new(id, ids, fast_raft_config());
        let driver = RaftDriver::new(node, Kv::default());
        let cfg = RuntimeConfig {
            tick_period: TICK_PERIOD,
            allow_join: false,
            allow_leave: false,
            ..RuntimeConfig::default()
        };
        let handle = spawn_node(driver, Arc::clone(&transport), &cfg);
        let service = NodeService::new(handle.clone(), Arc::clone(&transport));
        net.attach(id, Arc::new(service));
        handles.push((id, handle));
    }
    (net, handles)
}

#[tokio::test(start_paused = true)]
async fn typed_client_proposes_and_reads_through_any_node() {
    let (net, handles) = spawn_cluster();
    let leader = await_node_leader(&handles).await;

    let remote = RemoteClient::new(Arc::new(net.clone()), [NodeId(1), NodeId(2), NodeId(3)]);
    let client: TypedClient<RemoteClient, Kv> = TypedClient::new(remote);

    let resp = client
        .propose(&Cmd::Set {
            key: "a".into(),
            value: "1".into(),
        })
        .await
        .expect("propose");
    assert_eq!(resp, Resp::Set { previous: None });

    let resp = client
        .query(&Qry::Get { key: "a".into() })
        .await
        .expect("query");
    assert_eq!(resp, Resp::Value(Some("1".into())));

    assert!([NodeId(1), NodeId(2), NodeId(3)].contains(&leader));

    for (_, h) in &handles {
        h.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn client_targeting_only_a_follower_serves_reads_locally() {
    let (net, handles) = spawn_cluster();
    let leader = await_node_leader(&handles).await;

    let follower = handles
        .iter()
        .map(|(id, _)| *id)
        .find(|id| *id != leader)
        .unwrap();

    let remote = RemoteClient::new(Arc::new(net.clone()), [follower]);
    let client: TypedClient<RemoteClient, Kv> = TypedClient::new(remote);

    let resp = client
        .propose(&Cmd::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect("follower forwards write to leader");
    assert_eq!(resp, Resp::Set { previous: None });

    let resp = client
        .query(&Qry::Get { key: "k".into() })
        .await
        .expect("follower serves linearizable read locally after ReadIndex confirm");
    assert_eq!(resp, Resp::Value(Some("v".into())));

    for (_, h) in &handles {
        h.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn client_fails_over_when_the_first_target_is_unreachable() {
    let (net, handles) = spawn_cluster();
    let _leader = await_node_leader(&handles).await;

    assert!(net.detach(NodeId(1)));

    let remote = RemoteClient::new(Arc::new(net.clone()), [NodeId(1), NodeId(2), NodeId(3)])
        .with_retry(RetryPolicy {
            max_attempts: 8,
            attempt_timeout: Duration::from_secs(2),
            backoff: Duration::ZERO,
        });

    let payload = craft_actor::craft_proto::encode(&Cmd::Set {
        key: "x".into(),
        value: "y".into(),
    })
    .unwrap();
    let bytes = remote.propose(payload).await.expect("failover write");
    let resp: Resp = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(resp, Resp::Set { previous: None });

    for (_, h) in &handles {
        h.shutdown();
    }
}
