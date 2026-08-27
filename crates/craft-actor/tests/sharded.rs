//! Multi-Raft integration: shard-aware client routing through
//! [`ShardedNodeService`] (write-sharding-multi-raft).

use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_core::RaftGroupId;
use craft_actor::craft_net::{LocalNetwork, Transport, send_client_request};
use craft_actor::craft_proto::{ClientRequest, ClientResponse, NodeId};
use craft_actor::spawn_multi_raft_node;
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, find_keys_for_two_groups,
    wait_for_all_node_leaders,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_writes_route_to_independent_raft_groups() {
    let net = LocalNetwork::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let node_id = NodeId(1);
    let members = [node_id];
    let shard_count = 64;
    let group_ids = [RaftGroupId(0), RaftGroupId(1)];
    let (route_key_a, route_key_b) = find_keys_for_two_groups(shard_count, &group_ids);

    let machines = vec![KvMachine::default(), KvMachine::default()];
    let (sharded, handles) = spawn_multi_raft_node(
        node_id,
        &members,
        craft_actor::craft_core::DEFAULT_GROUP_REPLICATION_FACTOR,
        craft_actor::craft_core::Config::default(),
        craft_actor::RuntimeConfig {
            tick_period: TICK_PERIOD,
            allow_join: false,
            allow_leave: false,
        },
        shard_count,
        2,
        machines,
        Arc::clone(&transport),
        Duration::from_secs(5),
        None,
    )
    .expect("spawn multi-raft node");
    net.attach(node_id, sharded);

    wait_for_all_node_leaders(&handles).await;

    let cmd_a = craft_actor::craft_proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "group0".into(),
    })
    .unwrap();
    let cmd_b = craft_actor::craft_proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "group1".into(),
    })
    .unwrap();

    let resp_a = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_key_a.clone(),
            command: cmd_a,
        },
    )
    .await
    .expect("propose to group 0");
    let ClientResponse::Ok(bytes) = resp_a else {
        panic!("unexpected propose response: {resp_a:?}");
    };
    let set_a: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(set_a, KvResponse::Set { previous: None });

    let resp_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_key_b.clone(),
            command: cmd_b,
        },
    )
    .await
    .expect("propose to group 1");
    let ClientResponse::Ok(_bytes) = resp_b else {
        panic!("unexpected propose response: {resp_b:?}");
    };

    let qry_a = craft_actor::craft_proto::encode(&KvQuery::Get { key: "a".into() }).unwrap();
    let got_a = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_key_a,
            query: qry_a,
        },
    )
    .await
    .expect("query group 0");
    let ClientResponse::Ok(bytes) = got_a else {
        panic!("unexpected query response: {got_a:?}");
    };
    let val_a: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(val_a, KvResponse::Value(Some("group0".into())));

    let qry_b = craft_actor::craft_proto::encode(&KvQuery::Get { key: "a".into() }).unwrap();
    let got_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_key_b,
            query: qry_b,
        },
    )
    .await
    .expect("query group 1");
    let ClientResponse::Ok(bytes) = got_b else {
        panic!("unexpected query response: {got_b:?}");
    };
    let val_b: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(val_b, KvResponse::Value(Some("group1".into())));

    for handle in handles {
        handle.shutdown();
    }
}
