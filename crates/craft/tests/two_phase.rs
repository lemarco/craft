//! Cross-shard two-phase commit integration (optional Tier 2 increment).

use std::sync::Arc;

use craft::CraftCluster;
use craft::client::{RemoteClient, propose_cross_shard_2pc};
use craft::core::{RaftGroupId, StableShardRouter, TwoPhasePlan, TwoPhaseStep, place_shard};
use craft::net::{LocalNetwork, send_client_request};
use craft::proto::{ClientRequest, ClientResponse};
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, advance, await_craft_leader,
    fast_raft_config_with_seed, find_keys_for_two_groups, wait_for_each_group_cluster_leader,
};

async fn spawn_two_group_cluster_with_2pc() -> (LocalNetwork, Vec<Arc<CraftCluster<KvMachine>>>) {
    let ids = [craft::NodeId(1), craft::NodeId(2), craft::NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(17))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .cross_shard_2pc(true)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    (net, clusters)
}

fn two_group_plan(key_a: Vec<u8>, key_b: Vec<u8>) -> TwoPhasePlan {
    TwoPhasePlan {
        tx_id: b"transfer-2pc".to_vec(),
        steps: vec![
            TwoPhaseStep {
                key: key_a,
                command: craft::proto::encode(&KvCommand::Set {
                    key: "from".into(),
                    value: "100".into(),
                })
                .unwrap(),
            },
            TwoPhaseStep {
                key: key_b,
                command: craft::proto::encode(&KvCommand::Set {
                    key: "to".into(),
                    value: "200".into(),
                })
                .unwrap(),
            },
        ],
    }
}

#[tokio::test(start_paused = true)]
async fn cross_shard_two_phase_commits_two_groups() {
    let (net, clusters) = spawn_two_group_cluster_with_2pc().await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_craft_leader(&clusters).await;

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let router = StableShardRouter::new(64);
    let group_for_key = |key: &[u8]| {
        let shard = router.shard_for(key)?;
        place_shard(shard, &groups).map(|g| g.0)
    };

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    propose_cross_shard_2pc(
        &client,
        &two_group_plan(key_a.clone(), key_b.clone()),
        group_for_key,
    )
    .await
    .expect("2pc commits");

    let qry_from = craft::proto::encode(&KvQuery::Get { key: "from".into() }).unwrap();
    let got_from = send_client_request(
        &*Arc::new(net.clone()),
        leader.node_id(),
        &ClientRequest::QueryKeyed {
            key: key_a,
            query: qry_from,
        },
    )
    .await
    .expect("query from");
    let ClientResponse::Ok(bytes_from) = got_from else {
        panic!("unexpected {got_from:?}");
    };
    let val_from: KvResponse = craft::proto::decode(&bytes_from).unwrap();
    assert_eq!(val_from, KvResponse::Value(Some("100".into())));

    let qry_to = craft::proto::encode(&KvQuery::Get { key: "to".into() }).unwrap();
    let got_to = send_client_request(
        &*Arc::new(net.clone()),
        leader.node_id(),
        &ClientRequest::QueryKeyed {
            key: key_b,
            query: qry_to,
        },
    )
    .await
    .expect("query to");
    let ClientResponse::Ok(bytes_to) = got_to else {
        panic!("unexpected {got_to:?}");
    };
    let val_to: KvResponse = craft::proto::decode(&bytes_to).unwrap();
    assert_eq!(val_to, KvResponse::Value(Some("200".into())));

    for _ in 0..5 {
        advance(TICK_PERIOD).await;
    }
    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn cross_shard_two_phase_rejected_when_disabled() {
    let ids = [craft::NodeId(1), craft::NodeId(2), craft::NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(19))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_craft_leader(&clusters).await;

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let router = StableShardRouter::new(64);
    let group_for_key = |key: &[u8]| {
        let shard = router.shard_for(key)?;
        place_shard(shard, &groups).map(|g| g.0)
    };

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    let err = propose_cross_shard_2pc(&client, &two_group_plan(key_a, key_b), group_for_key)
        .await
        .expect_err("2pc disabled");
    assert!(err.to_string().contains("cross-shard 2PC is disabled"));

    for cluster in &clusters {
        cluster.shutdown();
    }
}
