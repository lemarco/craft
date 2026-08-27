//! Multi-Raft integration: shard-aware client routing through
//! [`ShardedNodeService`] (write-sharding-multi-raft).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_core::RaftGroupId;
use craft_actor::craft_core::ShardRoutingKind;
use craft_actor::craft_net::{LocalNetwork, RequestHandler, Transport, send_client_request};
use craft_actor::craft_proto::{ClientRequest, ClientResponse, NodeId};
use craft_actor::{RuntimeConfig, ShardedNodeService, spawn_multi_raft_node, spawn_raft_group};
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, fast_raft_config_with_seed,
    find_keys_for_two_groups, wait_for_all_node_leaders, wait_for_node_leader,
};

#[tokio::test(start_paused = true)]
async fn keyed_writes_route_to_independent_raft_groups() {
    let net = LocalNetwork::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let node_id = NodeId(1);
    let members = [node_id];
    let shard_count = 64;
    let group_ids = [RaftGroupId(0), RaftGroupId(1)];
    let (route_key_a, route_key_b) = find_keys_for_two_groups(shard_count, &group_ids);

    let machines = vec![KvMachine::default(), KvMachine::default()];
    let runtime = RuntimeConfig {
        tick_period: TICK_PERIOD,
        allow_join: false,
        allow_leave: false,
        ..RuntimeConfig::default()
    };
    let (sharded, handles) = spawn_multi_raft_node(
        node_id,
        &members,
        craft_actor::craft_core::DEFAULT_GROUP_REPLICATION_FACTOR,
        craft_actor::craft_core::Config::default(),
        runtime.clone(),
        runtime,
        shard_count,
        ShardRoutingKind::StableVirtual,
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

#[tokio::test(start_paused = true)]
async fn sharded_runtime_adopts_a_second_group_at_runtime() {
    let net = LocalNetwork::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let node_id = NodeId(1);
    let members = [node_id];
    let shard_count = 64;
    let group_ids = [RaftGroupId(0), RaftGroupId(1)];
    let (route_key_a, route_key_b) = find_keys_for_two_groups(shard_count, &group_ids);
    let runtime = RuntimeConfig {
        tick_period: TICK_PERIOD,
        allow_join: false,
        allow_leave: false,
        ..RuntimeConfig::default()
    };
    let raft = fast_raft_config_with_seed(3);

    let (service0, handle0) = spawn_raft_group(
        node_id,
        &members,
        0,
        raft.clone(),
        runtime.clone(),
        KvMachine::default(),
        Arc::clone(&transport),
        Duration::from_secs(5),
        None,
    )
    .expect("spawn group 0");

    let mut services = BTreeMap::new();
    services.insert(0, service0);
    let sharded = Arc::new(ShardedNodeService::new(
        shard_count,
        ShardRoutingKind::StableVirtual,
        vec![RaftGroupId(0)],
        services,
    ));
    net.attach(node_id, Arc::clone(&sharded) as Arc<dyn RequestHandler>);

    wait_for_node_leader(&handle0).await;

    let (service1, handle1) = spawn_raft_group(
        node_id,
        &members,
        1,
        raft,
        runtime,
        KvMachine::default(),
        Arc::clone(&transport),
        Duration::from_secs(5),
        None,
    )
    .expect("spawn group 1");
    sharded.insert_group(1, service1);
    wait_for_node_leader(&handle1).await;

    assert_eq!(
        sharded.hosted_group_ids(),
        vec![RaftGroupId(0), RaftGroupId(1)],
        "both groups should be registered on the sharded handler"
    );

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
    .expect("propose to adopted group 0");
    assert!(matches!(resp_a, ClientResponse::Ok(_)));

    let resp_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_key_b.clone(),
            command: cmd_b,
        },
    )
    .await
    .expect("propose to adopted group 1");
    assert!(matches!(resp_b, ClientResponse::Ok(_)));

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

    let removed = sharded.remove_group(0);
    assert!(removed.is_some(), "retire should drop group 0 handler");
    assert_eq!(sharded.hosted_group_ids(), vec![RaftGroupId(1)]);

    handle0.shutdown();
    handle1.shutdown();
}

#[tokio::test(start_paused = true)]
async fn stable_shard_activation_rejects_inactive_keys() {
    use craft_actor::craft_core::{ShardRoutingKind, StableShardRouter};

    let net = LocalNetwork::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let node_id = NodeId(1);
    let members = [node_id];
    let active = 8;
    let runtime = RuntimeConfig {
        tick_period: TICK_PERIOD,
        allow_join: false,
        allow_leave: false,
        ..RuntimeConfig::default()
    };
    let (sharded, handles) = spawn_multi_raft_node(
        node_id,
        &members,
        craft_actor::craft_core::DEFAULT_GROUP_REPLICATION_FACTOR,
        craft_actor::craft_core::Config::default(),
        runtime.clone(),
        runtime,
        active,
        ShardRoutingKind::StableVirtual,
        1,
        vec![KvMachine::default()],
        Arc::clone(&transport),
        Duration::from_secs(5),
        None,
    )
    .expect("spawn stable node");
    net.attach(node_id, Arc::clone(&sharded) as Arc<dyn RequestHandler>);

    let inactive_key = {
        let router = StableShardRouter::new(active);
        (0..50_000u32)
            .find_map(|i| {
                let key = format!("inactive-{i}").into_bytes();
                router.shard_for(&key).is_none().then_some(key)
            })
            .expect("inactive key")
    };

    let cmd = craft_actor::craft_proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "v".into(),
    })
    .unwrap();
    let resp = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: inactive_key,
            command: cmd,
        },
    )
    .await
    .expect("propose inactive key");
    assert!(
        matches!(resp, ClientResponse::Error(ref e) if e.contains("active shard")),
        "unexpected response: {resp:?}"
    );

    let plan = sharded.activate_shards(16).expect("activate");
    assert_eq!(plan.from, active);
    assert_eq!(plan.to, 16);

    for handle in handles {
        handle.shutdown();
    }
}
