//! Cross-shard two-phase commit integration (optional cross-shard 2PC increment).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use trembita::client::{
    RemoteClient, ResumeTwoPhaseOpts, TwoPhaseClient, propose_cross_shard_2pc,
    resume_cross_shard_2pc,
};
use trembita::cluster::TrembitaCluster;
use trembita::core::{RaftGroupId, StableShardRouter, TwoPhasePlan, TwoPhaseStep, place_shard};
use trembita::net::{LocalNetwork, send_client_request};
use trembita::proto::{ClientRequest, ClientResponse};
use trembita_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, advance, await_trembita_leader,
    fast_raft_config_with_seed, find_keys_for_two_groups, wait_for_each_group_cluster_leader,
    wait_for_trembita_stopped,
};

async fn spawn_two_group_cluster_with_2pc(
    durable: bool,
) -> (LocalNetwork, Vec<Arc<TrembitaCluster<KvMachine>>>) {
    let ids = [
        trembita::NodeId(1),
        trembita::NodeId(2),
        trembita::NodeId(3),
    ];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let mut builder = TrembitaCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(17))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .cross_shard_2pc(true)
            .raft_machines([KvMachine::default(), KvMachine::default()]);
        if durable {
            builder = builder.durable_cross_shard_2pc(true);
        }
        let cluster = builder.start_local(&net).await;
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
                command: trembita::proto::encode(&KvCommand::Set {
                    key: "from".into(),
                    value: "100".into(),
                })
                .unwrap(),
            },
            TwoPhaseStep {
                key: key_b,
                command: trembita::proto::encode(&KvCommand::Set {
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
    let (net, clusters) = spawn_two_group_cluster_with_2pc(false).await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_trembita_leader(&clusters).await;

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

    let qry_from = trembita::proto::encode(&KvQuery::Get { key: "from".into() }).unwrap();
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
    let val_from: KvResponse = trembita::proto::decode(&bytes_from).unwrap();
    assert_eq!(val_from, KvResponse::Value(Some("100".into())));

    let qry_to = trembita::proto::encode(&KvQuery::Get { key: "to".into() }).unwrap();
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
    let val_to: KvResponse = trembita::proto::decode(&bytes_to).unwrap();
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
    let ids = [
        trembita::NodeId(1),
        trembita::NodeId(2),
        trembita::NodeId(3),
    ];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = TrembitaCluster::builder(id, KvMachine::default())
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
    let leader = await_trembita_leader(&clusters).await;

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

fn node_data_dir(base: &Path, id: trembita::NodeId) -> PathBuf {
    base.join(format!("node-{}", id.0))
}

async fn spawn_durable_two_group_cluster_with_2pc(
    net: &LocalNetwork,
    id: trembita::NodeId,
    members: [trembita::NodeId; 3],
    data_dir: PathBuf,
    prepare_timeout: Option<Duration>,
) -> TrembitaCluster<KvMachine> {
    let mut builder = TrembitaCluster::builder(id, KvMachine::default())
        .members(members)
        .raft_config(fast_raft_config_with_seed(23))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .durable_cross_shard_2pc(true)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .data_dir(data_dir);
    if let Some(timeout) = prepare_timeout {
        builder = builder.two_phase_prepare_timeout(timeout);
    }
    builder.start_local(net).await
}

#[tokio::test(start_paused = true)]
async fn durable_cross_shard_two_phase_prepare_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let ids = [
        trembita::NodeId(1),
        trembita::NodeId(2),
        trembita::NodeId(3),
    ];

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let plan = two_group_plan(key_a.clone(), key_b.clone());

    {
        let mut clusters = Vec::new();
        for &id in &ids {
            let cluster = spawn_durable_two_group_cluster_with_2pc(
                &net,
                id,
                ids,
                node_data_dir(&base, id),
                None,
            )
            .await;
            clusters.push(Arc::new(cluster));
        }
        wait_for_each_group_cluster_leader(&clusters, 2).await;
        let leader = await_trembita_leader(&clusters).await;
        let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);

        for step in &plan.steps {
            client
                .prepare_keyed(plan.tx_id.clone(), step.key.clone(), step.command.clone())
                .await
                .expect("durable prepare");
        }

        for cluster in &clusters {
            wait_for_trembita_stopped(cluster.as_ref()).await;
        }
        for &id in &ids {
            let _ = net.detach(id);
        }
    }

    {
        let mut clusters = Vec::new();
        for &id in &ids {
            let cluster = spawn_durable_two_group_cluster_with_2pc(
                &net,
                id,
                ids,
                node_data_dir(&base, id),
                None,
            )
            .await;
            clusters.push(Arc::new(cluster));
        }
        wait_for_each_group_cluster_leader(&clusters, 2).await;
        let leader = await_trembita_leader(&clusters).await;
        let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
        let router = StableShardRouter::new(64);
        let group_for_key = |key: &[u8]| {
            let shard = router.shard_for(key)?;
            place_shard(shard, &groups).map(|g| g.0)
        };

        resume_cross_shard_2pc(&client, &plan, group_for_key, ResumeTwoPhaseOpts::default())
            .await
            .expect("resume commit after restart");

        let qry_from = trembita::proto::encode(&KvQuery::Get { key: "from".into() }).unwrap();
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
        let val_from: KvResponse = trembita::proto::decode(&bytes_from).unwrap();
        assert_eq!(val_from, KvResponse::Value(Some("100".into())));

        let qry_to = trembita::proto::encode(&KvQuery::Get { key: "to".into() }).unwrap();
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
        let val_to: KvResponse = trembita::proto::decode(&bytes_to).unwrap();
        assert_eq!(val_to, KvResponse::Value(Some("200".into())));

        for cluster in &clusters {
            cluster.shutdown();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn durable_two_phase_prepare_gc_aborts_stale() {
    let net = LocalNetwork::new();
    let ids = [
        trembita::NodeId(1),
        trembita::NodeId(2),
        trembita::NodeId(3),
    ];
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = TrembitaCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(29))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .durable_cross_shard_2pc(true)
            .two_phase_prepare_timeout(Duration::from_millis(50))
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_trembita_leader(&clusters).await;

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key_a, _) = find_keys_for_two_groups(64, &groups);
    let plan = two_group_plan(key_a.clone(), key_a); // only need one prepare
    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);

    client
        .prepare_keyed(
            plan.tx_id.clone(),
            plan.steps[0].key.clone(),
            plan.steps[0].command.clone(),
        )
        .await
        .expect("prepare");

    for _ in 0..12 {
        advance(TICK_PERIOD).await;
    }

    let err = client
        .commit_keyed(plan.tx_id.clone(), plan.steps[0].key.clone())
        .await
        .expect_err("stale prepare gc'd");
    assert!(
        err.to_string().contains("no prepared command"),
        "unexpected {err}"
    );

    for cluster in &clusters {
        cluster.shutdown();
    }
}
