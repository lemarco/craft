//! Multi-Raft via [`CraftClusterBuilder::raft_machines`] (write-sharding-multi-raft).

use std::sync::Arc;

use craft::CraftCluster;
use craft::core::RaftGroupId;
use craft::net::{LocalNetwork, send_client_request, send_group_migrate, send_join_request};
use craft::proto::{
    ClientRequest, ClientResponse, GroupMigrateRequest, JoinRequest, JoinResponse, NodeId,
    PROTOCOL_VERSION,
};
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, advance, await_craft_leader,
    fast_raft_config_with_seed, find_keys_for_two_groups, wait_for_each_group_cluster_leader,
    wait_for_group_leaders,
};

async fn spawn_three_node_multi_raft_cluster()
-> (LocalNetwork, [NodeId; 3], Vec<Arc<CraftCluster<KvMachine>>>) {
    spawn_multi_node_cluster(3, false).await
}

async fn spawn_three_node_multi_raft_cluster_allow_join()
-> (LocalNetwork, [NodeId; 3], Vec<Arc<CraftCluster<KvMachine>>>) {
    spawn_multi_node_cluster(3, true).await
}

async fn spawn_multi_node_cluster(
    node_count: u32,
    allow_join: bool,
) -> (LocalNetwork, [NodeId; 3], Vec<Arc<CraftCluster<KvMachine>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let shard_count = 64;
    let mut clusters = Vec::new();
    for &id in &ids[..node_count as usize] {
        let mut builder = CraftCluster::builder(id, KvMachine::default())
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

async fn wait_for_all_group_leaders(clusters: &[Arc<CraftCluster<KvMachine>>]) {
    let group_count = clusters.first().map(|c| c.raft_groups()).unwrap_or(0);
    wait_for_each_group_cluster_leader(clusters, group_count).await;
}

async fn cluster_leader(clusters: &[Arc<CraftCluster<KvMachine>>]) -> Arc<CraftCluster<KvMachine>> {
    await_craft_leader(clusters).await
}

async fn leave_via_cluster_rpc(joiner: &CraftCluster<KvMachine>, joiner_id: NodeId) {
    let membership = joiner.leave().await.expect("leave via facade");
    assert!(
        !membership.voters.contains(&joiner_id),
        "joiner still in group 0 voters: {membership:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn builder_hosts_independent_raft_groups() {
    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let shard_count = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(shard_count, &groups);

    let cluster = CraftCluster::builder(node_id, KvMachine::default())
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

    let transport: Arc<dyn craft::net::Transport> = Arc::new(net.clone());
    let cmd_a = craft::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "g0".into(),
    })
    .unwrap();
    let cmd_b = craft::proto::encode(&KvCommand::Set {
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

    let qry = craft::proto::encode(&KvQuery::Get { key: "k".into() }).unwrap();
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
    let val: KvResponse = craft::proto::decode(&bytes).unwrap();
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
    let val: KvResponse = craft::proto::decode(&bytes).unwrap();
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
    let cmd = craft::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "via-follower-read".into(),
    })
    .unwrap();
    let qry = craft::proto::encode(&KvQuery::Get { key: "k".into() }).unwrap();

    let transport: Arc<dyn craft::net::Transport> = Arc::new(net.clone());
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
    let val: KvResponse = craft::proto::decode(&bytes).unwrap();
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
        let cluster = CraftCluster::builder(node_id, KvMachine::default())
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

    let layout = craft::storage::GroupRedbLayout::new(&data_dir);
    let store = layout.open_group(0).unwrap();
    use craft::storage::LogStore;
    assert!(store.last_index().unwrap().0 >= 1);
}

#[tokio::test(start_paused = true)]
async fn wire_group_migrate_rpc_is_routed() {
    let net = LocalNetwork::new();
    let ids = [NodeId(1), NodeId(2)];

    let source = CraftCluster::builder(NodeId(1), KvMachine::default())
        .members(ids)
        .raft_config(fast_raft_config_with_seed(3))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    let target = CraftCluster::builder(NodeId(2), KvMachine::default())
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
    clusters: &[Arc<CraftCluster<KvMachine>>],
) -> (NodeId, CraftCluster<KvMachine>) {
    wait_for_all_group_leaders(clusters).await;
    let leader = cluster_leader(clusters).await;
    let joiner_id = NodeId(4);

    let joiner = CraftCluster::builder(joiner_id, KvMachine::default())
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
        node_id: joiner_id,
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
    clusters: &[Arc<CraftCluster<KvMachine>>],
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
    let cmd = craft::proto::encode(&KvCommand::Set {
        key: "partition".into(),
        value: "ok".into(),
    })
    .unwrap();
    let qry = craft::proto::encode(&KvQuery::Get {
        key: "partition".into(),
    })
    .unwrap();

    net.detach(follower_id);

    let transport: Arc<dyn craft::net::Transport> = Arc::new(net.clone());
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
    let val: KvResponse = craft::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(Some("ok".into())));

    for c in clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn tier1_shard_expansion_and_keyed_batch() {
    use craft_client::{KeyedBatchStep, RemoteClient, propose_keyed_batch};

    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let shard_count = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(shard_count, &groups);

    let cluster = CraftCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(fast_raft_config_with_seed(9))
        .tick_period(TICK_PERIOD)
        .shard_count(shard_count)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    wait_for_group_leaders(&cluster).await;

    let plan = cluster.expand_shard_count(128).expect("expand shards");
    assert_eq!(plan.from, 64);
    assert_eq!(plan.to, 128);
    assert_eq!(cluster.shard_count(), 128);

    let transport: Arc<dyn craft::net::Transport> = Arc::new(net.clone());
    let client = RemoteClient::new(transport, [node_id]);
    let cmd_a = craft::proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "1".into(),
    })
    .unwrap();
    let cmd_b = craft::proto::encode(&KvCommand::Set {
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
