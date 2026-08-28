//! Cross-shard saga integration (multi-Raft Phase 4).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crafty::CraftyCluster;
use crafty::StoreSagaJournal;
use crafty::actor::{ActorStateStore, InMemoryStore};
use crafty::client::{
    KeyedClient, RemoteClient, RetryPolicy, RunSagaOpts, SagaJournal, SagaJournalPhase,
    SagaOutcome, SagaPlan, SagaStep, run_saga,
};
use crafty::net::{LocalNetwork, Transport, TransportError, decode_body};
use crafty::proto::{ClientRequest, ClientResponse, NodeId};
use crafty_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, advance, assert_eq,
    await_crafty_leader, fast_raft_config_with_seed, find_keys_for_two_groups,
    wait_for_crafty_stopped, wait_for_each_group_cluster_leader,
};
use std::path::{Path, PathBuf};

async fn spawn_two_group_cluster() -> (LocalNetwork, Vec<Arc<CraftyCluster<KvMachine>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    (net, clusters)
}

fn two_shard_plan(key_a: Vec<u8>, key_b: Vec<u8>) -> SagaPlan {
    SagaPlan {
        saga_id: b"transfer-x".to_vec(),
        steps: vec![
            SagaStep {
                key: key_a,
                command: crafty::proto::encode(&KvCommand::Set {
                    key: "from".into(),
                    value: "100".into(),
                })
                .unwrap(),
                compensate: crafty::proto::encode(&KvCommand::Delete { key: "from".into() })
                    .unwrap(),
            },
            SagaStep {
                key: key_b,
                command: crafty::proto::encode(&KvCommand::Set {
                    key: "to".into(),
                    value: "200".into(),
                })
                .unwrap(),
                compensate: crafty::proto::encode(&KvCommand::Delete { key: "to".into() }).unwrap(),
            },
        ],
    }
}

/// Fail keyed forward proposes after `forward_ok` successes; compensate still uses inner.
struct FailAfterForward {
    inner: Arc<LocalNetwork>,
    forward_ok: u32,
    forward_calls: Arc<AtomicU32>,
}

impl Transport for FailAfterForward {
    fn send(
        &self,
        peer: NodeId,
        route: crafty::net::Route,
        body: crafty::net::transport::Body,
    ) -> crafty::net::transport::BoxFuture<
        'static,
        Result<crafty::net::transport::Body, TransportError>,
    > {
        let inner = Arc::clone(&self.inner);
        let forward_ok = self.forward_ok;
        let forward_calls = Arc::clone(&self.forward_calls);
        Box::pin(async move {
            if let Ok(ClientRequest::ProposeKeyed { command, .. }) = decode_body(&body) {
                let is_compensate = crafty::proto::decode::<KvCommand>(&command)
                    .is_ok_and(|cmd| matches!(cmd, KvCommand::Delete { .. }));
                if !is_compensate {
                    let n = forward_calls.fetch_add(1, Ordering::Relaxed);
                    if n >= forward_ok {
                        return Err(TransportError::Unreachable(peer));
                    }
                }
            }
            inner.send(peer, route, body).await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn cross_shard_saga_completes_two_groups() {
    let (net, clusters) = spawn_two_group_cluster().await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_crafty_leader(&clusters).await;

    let groups = [crafty::core::RaftGroupId(0), crafty::core::RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let journal = StoreSagaJournal::new(Arc::clone(&store));

    let outcome = run_saga(
        &client,
        &two_shard_plan(key_a.clone(), key_b.clone()),
        RunSagaOpts {
            journal: Some(&journal),
            catalog_version: Some(leader.catalog_version()),
            ..RunSagaOpts::default()
        },
    )
    .await
    .expect("saga completes");
    assert!(matches!(outcome, SagaOutcome::Completed(_)));

    let records = store
        .get("crafty:saga:transfer-x")
        .await
        .expect("journal read")
        .expect("journal record");
    let record = crafty::client::decode_journal_record(&records).expect("decode");
    assert_eq!(record.phase, SagaJournalPhase::Completed);

    let qry_from = crafty::proto::encode(&KvQuery::Get { key: "from".into() }).unwrap();
    let got_from = crafty::net::send_client_request(
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
    let val_from: KvResponse = crafty::proto::decode(&bytes_from).unwrap();
    assert_eq!(val_from, KvResponse::Value(Some("100".into())));

    let qry_to = crafty::proto::encode(&KvQuery::Get { key: "to".into() }).unwrap();
    let got_to = crafty::net::send_client_request(
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
    let val_to: KvResponse = crafty::proto::decode(&bytes_to).unwrap();
    assert_eq!(val_to, KvResponse::Value(Some("200".into())));

    for _ in 0..5 {
        advance(TICK_PERIOD).await;
    }
    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn cross_shard_saga_compensates_when_second_forward_fails() {
    let (net, clusters) = spawn_two_group_cluster().await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_crafty_leader(&clusters).await;

    let groups = [crafty::core::RaftGroupId(0), crafty::core::RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);

    let transport = Arc::new(FailAfterForward {
        inner: Arc::new(net.clone()),
        forward_ok: 1,
        forward_calls: Arc::new(AtomicU32::new(0)),
    });
    let client = RemoteClient::new(transport, [leader.node_id()]).with_retry(RetryPolicy {
        max_attempts: 1,
        ..RetryPolicy::default()
    });

    let outcome = run_saga(
        &client,
        &two_shard_plan(key_a.clone(), key_b),
        RunSagaOpts::default(),
    )
    .await
    .expect("compensated saga");

    let SagaOutcome::Compensated {
        failed_step,
        compensated_steps,
        ..
    } = outcome
    else {
        panic!("expected compensated outcome");
    };
    assert_eq!(failed_step, 1);
    assert_eq!(compensated_steps, 1);

    let qry = crafty::proto::encode(&KvQuery::Get { key: "from".into() }).unwrap();
    let got = crafty::net::send_client_request(
        &*Arc::new(net.clone()),
        leader.node_id(),
        &ClientRequest::QueryKeyed {
            key: key_a,
            query: qry,
        },
    )
    .await
    .expect("query");
    let ClientResponse::Ok(bytes) = got else {
        panic!("unexpected {got:?}");
    };
    let val: KvResponse = crafty::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(None));

    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn cross_shard_saga_resume_completes_second_step() {
    let (net, clusters) = spawn_two_group_cluster().await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_crafty_leader(&clusters).await;

    let groups = [crafty::core::RaftGroupId(0), crafty::core::RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let plan = two_shard_plan(key_a.clone(), key_b.clone());

    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let journal = StoreSagaJournal::new(Arc::clone(&store));
    journal
        .on_started(
            &plan.saga_id,
            plan.steps.len(),
            Some(leader.catalog_version()),
        )
        .await
        .expect("seed journal");
    journal
        .on_step_committed(&plan.saga_id, 0)
        .await
        .expect("seed first step");

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    let outcome = leader
        .resume_keyed_saga(&client, &plan, &journal)
        .await
        .expect("resume completes");
    assert!(matches!(outcome, SagaOutcome::Completed(_)));
    assert!(
        leader
            .metrics()
            .render()
            .contains("crafty_saga_completed_total")
    );

    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn run_keyed_saga_is_idempotent_when_journal_completed() {
    let (net, clusters) = spawn_two_group_cluster().await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_crafty_leader(&clusters).await;

    let groups = [crafty::core::RaftGroupId(0), crafty::core::RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let plan = two_shard_plan(key_a, key_b);

    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let journal = StoreSagaJournal::new(Arc::clone(&store));
    journal
        .on_started(
            &plan.saga_id,
            plan.steps.len(),
            Some(leader.catalog_version()),
        )
        .await
        .expect("seed");
    journal
        .on_completed(&plan.saga_id)
        .await
        .expect("seed complete");

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    let outcome = leader
        .run_keyed_saga(&client, &plan, &journal)
        .await
        .expect("idempotent replay");
    assert!(matches!(outcome, SagaOutcome::Completed(_)));

    for cluster in &clusters {
        cluster.shutdown();
    }
}

fn node_data_dir(base: &Path, id: NodeId) -> PathBuf {
    base.join(format!("node-{}", id.0))
}

async fn spawn_durable_two_group_cluster(
    net: &LocalNetwork,
    id: NodeId,
    members: [NodeId; 3],
    data_dir: PathBuf,
) -> CraftyCluster<KvMachine> {
    CraftyCluster::builder(id, KvMachine::default())
        .members(members)
        .raft_config(fast_raft_config_with_seed(11))
        .tick_period(TICK_PERIOD)
        .shard_count(64)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .data_dir(data_dir)
        .start_local(net)
        .await
}

#[tokio::test(start_paused = true)]
async fn cross_shard_saga_survives_coordinator_restart_via_group0_journal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];

    let groups = [crafty::core::RaftGroupId(0), crafty::core::RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let plan = two_shard_plan(key_a.clone(), key_b.clone());

    {
        let mut clusters = Vec::new();
        for &id in &ids {
            let cluster =
                spawn_durable_two_group_cluster(&net, id, ids, node_data_dir(&base, id)).await;
            clusters.push(Arc::new(cluster));
        }
        wait_for_each_group_cluster_leader(&clusters, 2).await;
        let leader = await_crafty_leader(&clusters).await;
        let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);

        client
            .propose_keyed(key_a.clone(), plan.steps[0].command.clone())
            .await
            .expect("step 0 forward");

        let journal = leader.saga_journal();
        journal
            .on_started(
                &plan.saga_id,
                plan.steps.len(),
                Some(leader.catalog_version()),
            )
            .await
            .expect("journal start");
        journal
            .on_step_committed(&plan.saga_id, 0)
            .await
            .expect("journal step 0");

        for cluster in &clusters {
            wait_for_crafty_stopped(cluster.as_ref()).await;
        }
        for &id in &ids {
            let _ = net.detach(id);
        }
    }

    {
        let mut clusters = Vec::new();
        for &id in &ids {
            let cluster =
                spawn_durable_two_group_cluster(&net, id, ids, node_data_dir(&base, id)).await;
            clusters.push(Arc::new(cluster));
        }
        wait_for_each_group_cluster_leader(&clusters, 2).await;
        let leader = await_crafty_leader(&clusters).await;
        let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
        let journal = leader.saga_journal();

        let loaded = journal.load(&plan.saga_id).await.expect("load");
        assert!(
            loaded.is_some(),
            "group 0 journal must survive restart without Redis"
        );
        assert_eq!(loaded.unwrap().completed_steps, 1);

        let outcome = leader
            .resume_keyed_saga(&client, &plan, journal.as_ref())
            .await
            .expect("resume after restart");
        assert!(matches!(outcome, SagaOutcome::Completed(_)));

        let qry_to = crafty::proto::encode(&KvQuery::Get { key: "to".into() }).unwrap();
        let got_to = crafty::net::send_client_request(
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
        let val_to: KvResponse = crafty::proto::decode(&bytes_to).unwrap();
        assert_eq!(val_to, KvResponse::Value(Some("200".into())));

        for cluster in &clusters {
            cluster.shutdown();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn run_keyed_saga_with_group0_journal_completes() {
    let (net, clusters) = spawn_two_group_cluster().await;
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_crafty_leader(&clusters).await;

    let groups = [crafty::core::RaftGroupId(0), crafty::core::RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let plan = two_shard_plan(key_a.clone(), key_b.clone());

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    let journal = leader.saga_journal();
    let outcome = leader
        .run_keyed_saga(&client, &plan, journal.as_ref())
        .await
        .expect("group0 journal saga");
    assert!(matches!(outcome, SagaOutcome::Completed(_)));

    let loaded = journal.load(&plan.saga_id).await.expect("load");
    let record = loaded.expect("journal record");
    assert_eq!(record.phase, SagaJournalPhase::Completed);
    assert_eq!(record.completed_steps, 2);

    for cluster in &clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn composite_saga_journal_mirrors_to_actor_state_store() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .actor_state_store(Arc::clone(&store))
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    wait_for_each_group_cluster_leader(&clusters, 2).await;
    let leader = await_crafty_leader(&clusters).await;
    let journal = leader.saga_journal();

    journal
        .on_started(b"mirror-saga", 2, Some(leader.catalog_version()))
        .await
        .expect("start");
    journal
        .on_step_committed(b"mirror-saga", 0)
        .await
        .expect("step 0");

    let mirrored = store
        .get("crafty:saga:mirror-saga")
        .await
        .expect("store read")
        .expect("mirrored journal bytes");
    let record = crafty::client::decode_journal_record(&mirrored).expect("decode");
    assert_eq!(record.completed_steps, 1);

    for cluster in &clusters {
        cluster.shutdown();
    }
}
