//! Saga soak: partial forward + full cluster restart + `resume_saga` loop (B-10c).
//!
//! Env: `SOAK_SAGA_SECS` (default 15), `SOAK_SAGA_SEED` (default 0x5A6A).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crafty::CraftyCluster;
use crafty::actor::{ActorStateStore, InMemoryStore};
use crafty::client::{KeyedClient, RemoteClient, SagaOutcome, SagaPlan, SagaStep};
use crafty::core::{RaftGroupId, Role};
use crafty::net::LocalNetwork;
use crafty::proto::NodeId;
use crafty_benchmarks::env_u64;
use crafty_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, fast_raft_config_with_seed,
    find_keys_for_two_groups,
};

fn node_data_dir(base: &Path, id: NodeId) -> std::path::PathBuf {
    base.join(format!("node-{}", id.0))
}

async fn spawn_cluster(
    net: &LocalNetwork,
    ids: [NodeId; 3],
    base: &Path,
) -> Vec<Arc<CraftyCluster<KvMachine>>> {
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .shard_count(64)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .data_dir(node_data_dir(base, id))
            .actor_state_store(Arc::clone(&store))
            .start_local(net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    clusters
}

async fn await_meta_leader(clusters: &[Arc<CraftyCluster<KvMachine>>]) -> Arc<CraftyCluster<KvMachine>> {
    for _ in 0..1000 {
        for c in clusters {
            if c.is_leader().await {
                return Arc::clone(c);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("soak_saga: no meta leader elected");
}

async fn await_group_leaders(clusters: &[Arc<CraftyCluster<KvMachine>>], group_count: u32) {
    for _ in 0..1000 {
        let mut ready = true;
        'groups: for g in 0..group_count {
            for c in clusters {
                let Some(handle) = c.group_handles().get(g as usize) else {
                    continue;
                };
                if let Some(status) = handle.status().await
                    && status.role == Role::Leader
                {
                    continue 'groups;
                }
            }
            ready = false;
            break;
        }
        if ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("soak_saga: not all raft groups elected a leader");
}

async fn stop_all(clusters: Vec<Arc<CraftyCluster<KvMachine>>>) {
    for c in clusters {
        c.shutdown_and_wait().await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn plan_for_round(round: u64, key_a: &[u8], key_b: &[u8]) -> SagaPlan {
    SagaPlan {
        saga_id: format!("soak-saga-{round}").into_bytes(),
        steps: vec![
            SagaStep {
                key: key_a.to_vec(),
                command: crafty::proto::encode(&KvCommand::Set {
                    key: "from".into(),
                    value: format!("{round}-a"),
                })
                .unwrap(),
                compensate: crafty::proto::encode(&KvCommand::Delete { key: "from".into() })
                    .unwrap(),
            },
            SagaStep {
                key: key_b.to_vec(),
                command: crafty::proto::encode(&KvCommand::Set {
                    key: "to".into(),
                    value: format!("{round}-b"),
                })
                .unwrap(),
                compensate: crafty::proto::encode(&KvCommand::Delete { key: "to".into() }).unwrap(),
            },
        ],
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let budget = Duration::from_secs(env_u64("SOAK_SAGA_SECS", 15));
    let base_seed = env_u64("SOAK_SAGA_SEED", 0x5A6A);

    println!("soak_saga: {budget:?} budget (seed {base_seed:#x})");

    let base = tempfile::tempdir().expect("tempdir");
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);

    let start = Instant::now();
    let mut rounds = 0u64;
    let mut resumes = 0u64;

    while start.elapsed() < budget {
        rounds += 1;
        let plan = plan_for_round(rounds ^ base_seed, &key_a, &key_b);

        let clusters = spawn_cluster(&net, ids, base.path()).await;
        await_group_leaders(&clusters, 2).await;
        let leader = await_meta_leader(&clusters).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
        client
            .propose_keyed(plan.steps[0].key.clone(), plan.steps[0].command.clone())
            .await
            .expect("forward step 0");

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

        stop_all(clusters).await;
        for &id in &ids {
            let _ = net.detach(id);
        }

        let clusters = spawn_cluster(&net, ids, base.path()).await;
        await_group_leaders(&clusters, 2).await;
        let leader = await_meta_leader(&clusters).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
        let journal = leader.saga_journal();
        let loaded = journal.load(&plan.saga_id).await.expect("load");
        assert!(
            loaded.is_some(),
            "soak_saga: journal must survive restart (round {rounds})"
        );
        assert_eq!(loaded.unwrap().completed_steps, 1);

        let outcome = leader
            .resume_keyed_saga(&client, &plan, journal.as_ref())
            .await
            .expect("resume");
        assert!(
            matches!(outcome, SagaOutcome::Completed(_)),
            "soak_saga: expected completed saga, got {outcome:?}"
        );
        resumes += 1;

        let qry = crafty::proto::encode(&KvQuery::Get { key: "to".into() }).unwrap();
        let got = client
            .query_keyed(key_b.clone(), qry)
            .await
            .expect("query to");
        let val: KvResponse = crafty::proto::decode(&got).unwrap();
        assert_eq!(
            val,
            KvResponse::Value(Some(format!("{}-b", rounds ^ base_seed))),
            "soak_saga: step 1 not applied"
        );

        stop_all(clusters).await;
        for &id in &ids {
            let _ = net.detach(id);
        }
    }

    let secs = start.elapsed().as_secs_f64();
    println!("soak_saga OK: rounds={rounds} resumes={resumes} in {secs:.1}s");
    assert!(resumes > 0, "soak_saga: expected at least one resume cycle");
}
