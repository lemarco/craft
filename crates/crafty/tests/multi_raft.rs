//! Multi-Raft via [`CraftyClusterBuilder::raft_machines`] (write-sharding-multi-raft).

use std::sync::Arc;

use crafty::CraftyCluster;
use crafty::core::RaftGroupId;
use crafty::net::{LocalNetwork, send_client_request, send_group_migrate, send_join_request};
use crafty::proto::{
    ClientRequest, ClientResponse, GroupMigrateRequest, JoinRequest, JoinResponse, NodeId,
    PROTOCOL_VERSION,
};
use crafty::storage::LogStore;
use crafty_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, advance, await_crafty_leader,
    eventually_async_default, fast_raft_config_with_seed, find_keys_for_two_groups,
    wait_for_each_group_cluster_leader, wait_for_group_leaders,
};

async fn spawn_three_node_multi_raft_cluster() -> (
    LocalNetwork,
    [NodeId; 3],
    Vec<Arc<CraftyCluster<KvMachine>>>,
) {
    spawn_multi_node_cluster(3, false).await
}

async fn spawn_three_node_multi_raft_cluster_allow_join() -> (
    LocalNetwork,
    [NodeId; 3],
    Vec<Arc<CraftyCluster<KvMachine>>>,
) {
    spawn_multi_node_cluster(3, true).await
}

async fn spawn_multi_node_cluster(
    node_count: u32,
    allow_join: bool,
) -> (
    LocalNetwork,
    [NodeId; 3],
    Vec<Arc<CraftyCluster<KvMachine>>>,
) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let shard_count = 64;
    let mut clusters = Vec::new();
    for &id in &ids[..node_count as usize] {
        let mut builder = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(3))
            .tick_period(TICK_PERIOD)
            .shard_count(shard_count)
            .group_replication_factor(64)
            .raft_machines([KvMachine::default(), KvMachine::default()]);
        if allow_join {
            builder = builder.allow_join(true).allow_leave(true);
        }
        let cluster = builder.start_local(&net).await;
        clusters.push(Arc::new(cluster));
    }
    (net, ids, clusters)
}

async fn wait_for_all_group_leaders(clusters: &[Arc<CraftyCluster<KvMachine>>]) {
    let group_count = clusters.first().map_or(0, |c| c.raft_groups());
    wait_for_each_group_cluster_leader(clusters, group_count).await;
}

async fn cluster_leader(
    clusters: &[Arc<CraftyCluster<KvMachine>>],
) -> Arc<CraftyCluster<KvMachine>> {
    await_crafty_leader(clusters).await
}

async fn leave_via_cluster_rpc(joiner: &CraftyCluster<KvMachine>, joiner_id: NodeId) {
    let membership = joiner.leave().await.expect("leave via facade");
    assert!(
        !membership.voters.contains(&joiner_id),
        "joiner still in meta raft voters: {membership:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn builder_hosts_independent_raft_groups() {
    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let shard_count = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(shard_count, &groups);

    let cluster = CraftyCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(fast_raft_config_with_seed(3))
        .tick_period(TICK_PERIOD)
        .shard_count(shard_count)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    assert_eq!(cluster.raft_groups(), 2);
    assert_eq!(cluster.group_handles().len(), 2);

    wait_for_group_leaders(&cluster).await;

    let transport: Arc<dyn crafty::net::Transport> = Arc::new(net.clone());
    let cmd_a = crafty::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "g0".into(),
    })
    .unwrap();
    let cmd_b = crafty::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "g1".into(),
    })
    .unwrap();

    let resp = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_a.clone(),
            command: cmd_a,
        },
    )
    .await
    .expect("propose group 0");
    assert!(matches!(resp, ClientResponse::Ok(_)));

    let resp = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_b.clone(),
            command: cmd_b,
        },
    )
    .await
    .expect("propose group 1");
    assert!(matches!(resp, ClientResponse::Ok(_)));

    let qry = crafty::proto::encode(&KvQuery::Get { key: "k".into() }).unwrap();
    let got_a = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_a,
            query: qry.clone(),
        },
    )
    .await
    .expect("query group 0");
    let ClientResponse::Ok(bytes) = got_a else {
        panic!("unexpected response: {got_a:?}");
    };
    let val: KvResponse = crafty::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(Some("g0".into())));

    let got_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_b,
            query: qry,
        },
    )
    .await
    .expect("query group 1");
    let ClientResponse::Ok(bytes) = got_b else {
        panic!("unexpected response: {got_b:?}");
    };
    let val: KvResponse = crafty::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(Some("g1".into())));

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn follower_serves_keyed_reads_in_multi_raft_cluster() {
    let (net, ids, clusters) = spawn_three_node_multi_raft_cluster().await;
    wait_for_all_group_leaders(&clusters).await;

    let leader = cluster_leader(&clusters).await;
    let follower = ids
        .into_iter()
        .find(|id| *id != leader.node_id())
        .expect("follower node");

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, _) = find_keys_for_two_groups(64, &groups);
    let cmd = crafty::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "via-follower-read".into(),
    })
    .unwrap();
    let qry = crafty::proto::encode(&KvQuery::Get { key: "k".into() }).unwrap();

    let transport: Arc<dyn crafty::net::Transport> = Arc::new(net.clone());
    let wrote = send_client_request(
        &*transport,
        leader.node_id(),
        &ClientRequest::ProposeKeyed {
            key: route_a.clone(),
            command: cmd,
        },
    )
    .await
    .expect("propose on leader");
    assert!(matches!(wrote, ClientResponse::Ok(_)));

    let read = send_client_request(
        &*transport,
        follower,
        &ClientRequest::QueryKeyed {
            key: route_a,
            query: qry,
        },
    )
    .await
    .expect("follower keyed read");
    let ClientResponse::Ok(bytes) = read else {
        panic!("unexpected follower read response: {read:?}");
    };
    let val: KvResponse = crafty::proto::decode(&bytes).unwrap();
    assert_eq!(
        val,
        KvResponse::Value(Some("via-follower-read".into())),
        "follower should serve linearizable read locally after ReadIndex confirm"
    );

    for c in clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn builder_persists_each_raft_group_to_separate_redb_files() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let node_id = NodeId(1);

    {
        let cluster = CraftyCluster::builder(node_id, KvMachine::default())
            .members([node_id])
            .tick_period(TICK_PERIOD)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_group_leaders(&cluster).await;

        let resp = cluster.group_handles()[0]
            .propose(KvCommand::Set {
                key: "persist".into(),
                value: "g0".into(),
            })
            .await
            .expect("propose on group 0");
        assert_eq!(resp, KvResponse::Set { previous: None });

        cluster.shutdown_and_wait().await;
    }

    let layout = crafty::storage::GroupRedbLayout::new(&data_dir);
    let store = layout.open_group(0).unwrap();
    assert!(store.last_index().unwrap().0 >= 1);
}

#[tokio::test(start_paused = true)]
async fn wire_group_migrate_rpc_is_routed() {
    let net = LocalNetwork::new();
    let ids = [NodeId(1), NodeId(2)];

    let source = CraftyCluster::builder(NodeId(1), KvMachine::default())
        .members(ids)
        .raft_config(fast_raft_config_with_seed(3))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    let target = CraftyCluster::builder(NodeId(2), KvMachine::default())
        .members(ids)
        .raft_config(fast_raft_config_with_seed(3))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    wait_for_group_leaders(&source).await;

    let resp = source.group_handles()[0]
        .propose(KvCommand::Set {
            key: "wire".into(),
            value: "ok".into(),
        })
        .await
        .expect("propose on source group 0");
    assert_eq!(resp, KvResponse::Set { previous: None });

    let bundle = source.group_handles()[0]
        .export_migration()
        .await
        .expect("export migration bundle");

    let reply = send_group_migrate(
        &net,
        NodeId(2),
        &GroupMigrateRequest {
            group: 0,
            from: NodeId(1),
            bundle,
        },
    )
    .await
    .expect("group migrate rpc");
    assert!(reply.adopted, "migrate failed: {:?}", reply.error);

    source.shutdown();
    target.shutdown();
}

#[tokio::test(start_paused = true)]
async fn join_syncs_non_coordinator_group_membership() {
    let (net, ids, clusters) = spawn_three_node_multi_raft_cluster_allow_join().await;
    let (joiner_id, joiner) = join_fourth_node(&net, ids, &clusters).await;
    wait_for_group_voter(&clusters, 1, joiner_id, true).await;
    joiner.shutdown();
    for c in clusters {
        c.shutdown();
    }
}

async fn join_fourth_node(
    net: &LocalNetwork,
    ids: [NodeId; 3],
    clusters: &[Arc<CraftyCluster<KvMachine>>],
) -> (NodeId, CraftyCluster<KvMachine>) {
    wait_for_all_group_leaders(clusters).await;
    let leader = cluster_leader(clusters).await;
    let joiner_id = NodeId(4);

    let joiner = CraftyCluster::builder(joiner_id, KvMachine::default())
        .members(ids)
        .raft_config(fast_raft_config_with_seed(3))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .group_replication_factor(64)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .allow_join(true)
        .allow_leave(true)
        .start_local(net)
        .await;

    let request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: Some(joiner_id),
        advertise_addr: "node4.local:7443".to_string(),
    };
    let response = send_join_request(net, leader.node_id(), &request)
        .await
        .expect("join request");
    assert!(
        matches!(response, JoinResponse::Accepted { .. }),
        "join rejected: {response:?}"
    );
    (joiner_id, joiner)
}

async fn wait_for_group_voter(
    clusters: &[Arc<CraftyCluster<KvMachine>>],
    group: u32,
    voter: NodeId,
    present: bool,
) {
    for _ in 0..1000 {
        let mut active = 0usize;
        let mut matched = 0usize;
        for cluster in clusters {
            let Some(handle) = cluster.group_handle(group) else {
                continue;
            };
            let Some(status) = handle.status().await else {
                continue;
            };
            active += 1;
            if status.voters.contains(&voter) == present {
                matched += 1;
            }
        }
        if present {
            if active > 0 && matched == active {
                return;
            }
        } else if active > 0 && matched == active {
            return;
        }
        advance(TICK_PERIOD).await;
    }
    panic!("group {group} voter {voter:?} present={present} did not converge");
}

#[tokio::test(start_paused = true)]
async fn leave_syncs_non_coordinator_group_membership() {
    let (net, ids, clusters) = spawn_three_node_multi_raft_cluster_allow_join().await;
    let (joiner_id, joiner) = join_fourth_node(&net, ids, &clusters).await;
    wait_for_group_voter(&clusters, 1, joiner_id, true).await;

    leave_via_cluster_rpc(&joiner, joiner_id).await;

    joiner.shutdown();
    wait_for_group_voter(&clusters, 0, joiner_id, false).await;
    wait_for_group_voter(&clusters, 1, joiner_id, false).await;
    for c in clusters {
        c.shutdown();
    }
}

/// Partition a follower; keyed traffic on another group still commits, then heal.
#[tokio::test(start_paused = true)]
async fn multi_raft_survives_follower_partition() {
    let (net, ids, clusters) = spawn_three_node_multi_raft_cluster().await;
    wait_for_all_group_leaders(&clusters).await;

    let leader_cluster = cluster_leader(&clusters).await;
    let leader_id = leader_cluster.node_id();
    let follower_id = ids
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("follower");
    let follower_cluster = clusters
        .iter()
        .find(|c| c.node_id() == follower_id)
        .expect("follower cluster");
    let follower_handler = follower_cluster.wire_handler();

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (_, route_key) = find_keys_for_two_groups(64, &groups);
    let cmd = crafty::proto::encode(&KvCommand::Set {
        key: "partition".into(),
        value: "ok".into(),
    })
    .unwrap();
    let qry = crafty::proto::encode(&KvQuery::Get {
        key: "partition".into(),
    })
    .unwrap();

    let _ = net.detach(follower_id);

    let transport: Arc<dyn crafty::net::Transport> = Arc::new(net.clone());
    let resp = send_client_request(
        &*transport,
        leader_id,
        &ClientRequest::ProposeKeyed {
            key: route_key.clone(),
            command: cmd,
        },
    )
    .await
    .expect("keyed propose during follower partition");
    assert!(
        matches!(resp, ClientResponse::Ok(_)),
        "partition write failed: {resp:?}"
    );

    net.attach(follower_id, follower_handler);
    wait_for_all_group_leaders(&clusters).await;

    let read = send_client_request(
        &*transport,
        follower_id,
        &ClientRequest::QueryKeyed {
            key: route_key,
            query: qry,
        },
    )
    .await
    .expect("follower read after partition heal");
    let ClientResponse::Ok(bytes) = read else {
        panic!("unexpected read after heal: {read:?}");
    };
    let val: KvResponse = crafty::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(Some("ok".into())));

    for c in clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn tier1_shard_expansion_and_keyed_batch() {
    use crafty_client::{KeyedBatchStep, RemoteClient, propose_keyed_batch};

    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let shard_count = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(shard_count, &groups);

    let cluster = CraftyCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(fast_raft_config_with_seed(9))
        .tick_period(TICK_PERIOD)
        .shard_count(shard_count)
        .modulus_shards()
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    wait_for_group_leaders(&cluster).await;

    let plan = cluster.expand_shard_count(128).expect("expand shards");
    assert_eq!(plan.from, 64);
    assert_eq!(plan.to, 128);
    assert_eq!(cluster.shard_count(), 128);

    let transport: Arc<dyn crafty::net::Transport> = Arc::new(net.clone());
    let client = RemoteClient::new(transport, [node_id]);
    let cmd_a = crafty::proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "1".into(),
    })
    .unwrap();
    let cmd_b = crafty::proto::encode(&KvCommand::Set {
        key: "b".into(),
        value: "2".into(),
    })
    .unwrap();

    let results = propose_keyed_batch(
        &client,
        &[
            KeyedBatchStep {
                key: route_a,
                payload: cmd_a,
            },
            KeyedBatchStep {
                key: route_b,
                payload: cmd_b,
            },
        ],
    )
    .await
    .expect("batch propose");
    assert_eq!(results.len(), 2);

    cluster.shutdown();
}

fn find_key_for_group(shard_count: u32, groups: &[RaftGroupId], target: u32) -> Vec<u8> {
    use crafty::core::{StableShardRouter, place_shard};

    let router = StableShardRouter::new(shard_count);
    for i in 0..50_000u32 {
        let key = format!("catalog-{target}-{i}").into_bytes();
        let Some(shard) = router.shard_for(&key) else {
            continue;
        };
        if place_shard(shard, groups).is_some_and(|g| g.0 == target) {
            return key;
        }
    }
    panic!("no routing key for group {target}");
}

#[tokio::test(start_paused = true)]
async fn stable_shard_activation_preserves_key_routing() {
    use crafty::core::{
        ShardRoutingKind, StableShardRouter, place_shard, stable_router_preserves_routable_keys,
    };
    use crafty_client::{KeyedBatchStep, RemoteClient, propose_keyed_batch};

    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let active = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(active, &groups);

    let cluster = CraftyCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(fast_raft_config_with_seed(11))
        .tick_period(TICK_PERIOD)
        .shard_count(active)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    assert_eq!(cluster.shard_routing(), ShardRoutingKind::StableVirtual);
    wait_for_group_leaders(&cluster).await;

    let before_router = StableShardRouter::new(active);
    let shard_a = before_router.shard_for(&route_a).expect("route_a active");
    let shard_b = before_router.shard_for(&route_b).expect("route_b active");
    assert_eq!(place_shard(shard_a, &groups).unwrap().0, 0);
    assert_eq!(place_shard(shard_b, &groups).unwrap().0, 1);

    let plan = cluster.activate_shards(128).expect("activate shards");
    assert_eq!(plan.from, 64);
    assert_eq!(plan.to, 128);
    assert_eq!(cluster.shard_count(), 128);

    let after_router = StableShardRouter::new(128);
    assert_eq!(after_router.shard_for(&route_a), Some(shard_a));
    assert_eq!(after_router.shard_for(&route_b), Some(shard_b));
    assert!(stable_router_preserves_routable_keys(
        active,
        128,
        &[route_a.as_slice(), route_b.as_slice()],
    ));

    let transport: Arc<dyn crafty::net::Transport> = Arc::new(net.clone());
    let client = RemoteClient::new(transport, [node_id]);
    let cmd_a = crafty::proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "1".into(),
    })
    .unwrap();
    let cmd_b = crafty::proto::encode(&KvCommand::Set {
        key: "b".into(),
        value: "2".into(),
    })
    .unwrap();

    let results = propose_keyed_batch(
        &client,
        &[
            KeyedBatchStep {
                key: route_a,
                payload: cmd_a,
            },
            KeyedBatchStep {
                key: route_b,
                payload: cmd_b,
            },
        ],
    )
    .await
    .expect("batch propose after activation");
    assert_eq!(results.len(), 2);

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn add_raft_groups_expands_catalog_without_restart() {
    let (net, _ids, clusters) = spawn_three_node_multi_raft_cluster().await;
    wait_for_all_group_leaders(&clusters).await;

    let leader = cluster_leader(&clusters).await;
    assert_eq!(leader.raft_groups(), 2);
    assert_eq!(leader.catalog_version(), 1);

    let new_groups = leader.add_raft_groups(1).await.expect("add raft group");
    assert_eq!(new_groups, vec![2]);
    assert_eq!(leader.catalog_version(), 2);

    for _ in 0..40 {
        advance(TICK_PERIOD).await;
    }

    for cluster in &clusters {
        assert_eq!(
            cluster.raft_groups(),
            3,
            "node {} catalog",
            cluster.node_id().0
        );
    }

    let contact = clusters[0].node_id();
    let transport: Arc<dyn crafty::net::Transport> = Arc::new(net.clone());
    let groups = [RaftGroupId(0), RaftGroupId(1), RaftGroupId(2)];
    let route_g2 = find_key_for_group(64, &groups, 2);
    let cmd = crafty::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "g2".into(),
    })
    .unwrap();
    let resp = send_client_request(
        &*transport,
        contact,
        &ClientRequest::ProposeKeyed {
            key: route_g2,
            command: cmd,
        },
    )
    .await
    .expect("propose group 2");
    assert!(matches!(resp, ClientResponse::Ok(_)));

    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn switch_to_stable_shards_from_modulus() {
    use crafty::client::RemoteClient;
    use crafty::core::ShardRoutingKind;
    use crafty_test_support::{KvCommand, KvMachine, TICK_PERIOD, fast_raft_config_with_seed};

    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let cluster = CraftyCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(fast_raft_config_with_seed(12))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .modulus_shards()
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    wait_for_group_leaders(&cluster).await;
    assert_eq!(cluster.shard_routing(), ShardRoutingKind::Modulus);

    let plan = cluster
        .switch_to_stable_shards()
        .expect("switch routing mode");
    assert_eq!(plan.from, ShardRoutingKind::Modulus);
    assert_eq!(plan.to, ShardRoutingKind::StableVirtual);
    assert_eq!(plan.active_count, 64);
    assert_eq!(cluster.shard_routing(), ShardRoutingKind::StableVirtual);
    assert!(cluster.switch_to_stable_shards().is_err());

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key, _) = find_keys_for_two_groups(64, &groups);

    let cmd = crafty::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "v".into(),
    })
    .unwrap();
    let resp = send_client_request(
        &*Arc::new(net.clone()),
        node_id,
        &ClientRequest::ProposeKeyed { key, command: cmd },
    )
    .await
    .expect("propose after switch");
    assert!(matches!(resp, ClientResponse::Ok(_)));
    let _client = RemoteClient::new(Arc::new(net.clone()), [node_id]);

    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn per_group_learners_replicate_without_voting() {
    let ids = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .group_replication_factor(3)
            .group_learner_factor(1)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    wait_for_each_group_cluster_leader(&clusters, 2).await;

    eventually_async_default("learners committed on group 0", || async {
        for c in &clusters {
            let Some(handle) = c.group_handle(0) else {
                continue;
            };
            if let Some(status) = handle.status().await
                && !status.learners.is_empty()
            {
                return true;
            }
        }
        false
    })
    .await;

    let live: Vec<_> = ids.to_vec();
    let learner_id = ids
        .into_iter()
        .find(|&id| {
            let voters = crafty::core::group_voters(RaftGroupId(0), &live, 3);
            let learners = crafty::core::group_learners(RaftGroupId(0), &live, 3, 1);
            learners.contains(&id) && !voters.contains(&id)
        })
        .expect("planner assigns a learner-only node for group 0");
    let learner = clusters
        .iter()
        .find(|c| c.node_id() == learner_id)
        .expect("learner cluster");

    assert!(
        learner.hosted_groups().contains(&0),
        "learner node must host group 0, hosted={:?}",
        learner.hosted_groups()
    );

    let handle = learner.group_handle(0).expect("group 0 handle");
    let status = handle.status().await.expect("status");
    assert!(status.learners.contains(&learner_id));
    assert!(!status.voters.contains(&learner_id));
    assert_ne!(status.role, crafty::core::Role::Leader);

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key, _) = find_keys_for_two_groups(64, &groups);
    let cmd = crafty::proto::encode(&KvCommand::Set {
        key: "learner-catchup".into(),
        value: "ok".into(),
    })
    .unwrap();
    let leader = await_crafty_leader(&clusters).await;
    let resp = send_client_request(
        &*Arc::new(net.clone()),
        leader.node_id(),
        &ClientRequest::ProposeKeyed { key, command: cmd },
    )
    .await
    .expect("propose");
    assert!(matches!(resp, ClientResponse::Ok(_)));

    eventually_async_default("learner group 0 catches up", || async {
        let Some(status) = handle.status().await else {
            return false;
        };
        status.last_applied.0 >= 1
    })
    .await;

    for c in &clusters {
        c.shutdown();
    }
}
