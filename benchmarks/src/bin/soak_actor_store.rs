//! Actor workflow store soak: `RedbActorStateStore` writes + full cluster restart loop (B-10a).
//!
//! Env: `SOAK_ACTOR_STORE_SECS` (default 15), `SOAK_ACTOR_STORE_SEED` (default 0xAC700).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use trembita::core::{Config, StateMachine};
use trembita::net::LocalNetwork;
use trembita::proto::LogIndex;
use trembita::cluster::TrembitaCluster;
use trembita::NodeId;
use trembita_benchmarks::env_u64;

#[derive(Default)]
struct Empty;

impl StateMachine for Empty {
    type Command = ();
    type Query = ();
    type Response = ();
    type Error = std::convert::Infallible;

    fn apply(&mut self, _index: LogIndex, _command: &()) -> Result<(), Self::Error> {
        Ok(())
    }
    fn query(&self, _query: &()) -> Result<(), Self::Error> {
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn raft_config(seed: u64) -> Config {
    Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed,
        ..Default::default()
    }
}

fn node_dir(base: &Path, id: NodeId) -> std::path::PathBuf {
    base.join(format!("node-{}", id.0))
}

async fn spawn_all(
    net: &LocalNetwork,
    ids: [NodeId; 3],
    base: &Path,
    seed: u64,
) -> Vec<Arc<TrembitaCluster<Empty>>> {
    let mut clusters = Vec::new();
    for &id in &ids {
        let data_dir = node_dir(base, id);
        std::fs::create_dir_all(&data_dir).expect("mkdir");
        let cluster = TrembitaCluster::builder(id, Empty)
            .members(ids)
            .raft_config(raft_config(seed ^ id.0))
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .data_dir(&data_dir)
            .start_local(net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    clusters
}

async fn await_leader(clusters: &[Arc<TrembitaCluster<Empty>>]) -> NodeId {
    for _ in 0..500 {
        for c in clusters {
            if c.is_leader().await {
                return c.node_id();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("soak_actor_store: no leader elected");
}

async fn store_set_with_retry(
    leader: &TrembitaCluster<Empty>,
    key: &str,
    value: &[u8],
) {
    for attempt in 0..100 {
        match leader
            .actor_state_store()
            .expect("leader store")
            .set(key, value, None)
            .await
        {
            Ok(()) => return,
            Err(e) if format!("{e}").contains("no raft leader") && attempt + 1 < 100 => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("set: {e}"),
        }
    }
}

async fn verify_key(clusters: &[Arc<TrembitaCluster<Empty>>], key: &str, value: &[u8]) {
    for c in clusters {
        let store = c.actor_state_store().expect("store on node");
        let got = store.get(key).await.expect("get");
        assert_eq!(
            got.as_deref(),
            Some(value),
            "node {:?} missing key {key}",
            c.node_id()
        );
    }
}

async fn stop_all(clusters: Vec<Arc<TrembitaCluster<Empty>>>) {
    for c in clusters {
        c.shutdown_and_wait().await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let budget = Duration::from_secs(env_u64("SOAK_ACTOR_STORE_SECS", 15));
    let base_seed = env_u64("SOAK_ACTOR_STORE_SEED", 0xAC700);

    println!("soak_actor_store: {budget:?} budget (seed {base_seed:#x})");

    let base = tempfile::tempdir().expect("tempdir");
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();

    let start = Instant::now();
    let mut rounds = 0u64;
    let mut writes = 0u64;
    let mut restarts = 0u64;
    let mut clusters = spawn_all(&net, ids, base.path(), base_seed).await;

    while start.elapsed() < budget {
        rounds += 1;
        let leader_id = await_leader(&clusters).await;
        let leader = clusters
            .iter()
            .find(|c| c.node_id() == leader_id)
            .expect("leader cluster");
        let key = format!("soak-{rounds}");
        let value = format!("v-{rounds}-{base_seed}").into_bytes();

        store_set_with_retry(leader, &key, &value).await;
        writes += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
        verify_key(&clusters, &key, &value).await;

        stop_all(clusters).await;
        for &id in &ids {
            let _ = net.detach(id);
        }
        restarts += 1;

        clusters = spawn_all(&net, ids, base.path(), base_seed ^ rounds).await;
        await_leader(&clusters).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        verify_key(&clusters, &key, &value).await;
    }

    stop_all(clusters).await;

    let secs = start.elapsed().as_secs_f64();
    println!(
        "soak_actor_store OK: rounds={rounds} writes={writes} restarts={restarts} in {secs:.1}s"
    );
    assert!(writes > 0, "soak_actor_store: expected at least one write");
    assert!(restarts > 0, "soak_actor_store: expected at least one restart");
}
