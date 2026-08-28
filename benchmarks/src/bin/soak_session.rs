//! Session soak: sticky [`ActorSession`] casts + worker node restart loop (B-10b).
//!
//! Exercises supervisor re-placement and client re-open after `NoTarget` — the same
//! path WebSocket gateways hit when a worker VPS dies (without running HTTP/WS here).
//!
//! Env: `SOAK_SESSION_SECS` (default 15), `SOAK_SESSION_SEED` (default 0x5E5510).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crafty::actor::{UserActor, remote_actor};
use crafty::core::{Config, StateMachine};
use crafty::net::LocalNetwork;
use crafty::proto::LogIndex;
use crafty::{CraftyCluster, NodeId};
use crafty_benchmarks::env_u64;

static HANDLED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct WorkerErr;
impl std::fmt::Display for WorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerErr {}

struct SessionWorker;

#[remote_actor]
impl UserActor for SessionWorker {
    type Config = u32;
    type Message = u64;
    type Error = WorkerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(SessionWorker)
    }

    fn handle(
        &mut self,
        _job: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        HANDLED.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(()))
    }
}

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

async fn await_workers(clusters: &[Arc<CraftyCluster<Empty>>], count: usize) {
    for _ in 0..1000 {
        if clusters[0].directory().lookup("w").len() >= count {
            let local_ok = clusters.iter().all(|c| c.registry().contains("w"));
            if local_ok {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("soak_session: workers not ready (need {count} in directory + local registry)");
}

use crafty::actor::CastError;

fn cast_error_retryable(err: &CastError) -> bool {
    match err {
        CastError::NoTarget(_) => true,
        CastError::Remote(e) => {
            let msg = format!("{e}");
            msg.contains("no actor named")
        }
        _ => false,
    }
}

async fn cast_session_round(cluster: &CraftyCluster<Empty>, user: &String, n: u64) -> u64 {
    let dir = cluster.directory();
    let cluster_view = dir.cluster("w");
    let session = cluster_view
        .session_keyed(user, Some(Duration::from_secs(60)))
        .expect("session");
    let mut ok = 0u64;
    for msg in 0..n {
        let payload = crafty::proto::encode(&(msg as u64)).unwrap();
        for attempt in 0..50 {
            match cluster.messaging().cast_session(&session, payload.clone()).await {
                Ok(()) => {
                    ok += 1;
                    break;
                }
                Err(e) if cast_error_retryable(&e) && attempt + 1 < 50 => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("soak_session: unexpected cast error: {e}"),
            }
        }
    }
    ok
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let budget = Duration::from_secs(env_u64("SOAK_SESSION_SECS", 15));
    let base_seed = env_u64("SOAK_SESSION_SEED", 0x5E5510);

    println!("soak_session: {budget:?} budget (seed {base_seed:#x})");

    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters: Vec<Arc<CraftyCluster<Empty>>> = Vec::new();

    for &id in &ids {
        let cluster = CraftyCluster::builder(id, Empty)
            .members(ids)
            .raft_config(raft_config(base_seed ^ id.0))
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20))
            .manage_auto::<SessionWorker>("w", 0)
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    await_workers(&clusters, 3).await;

    let start = Instant::now();
    let mut rounds = 0u64;
    let mut casts_ok = 0u64;
    let mut restarts = 0u64;
    let victim = NodeId(2);

    while start.elapsed() < budget {
        rounds += 1;
        let user = format!("tenant-{}", rounds ^ base_seed);
        casts_ok += cast_session_round(&clusters[0], &user, 4).await;

        let idx = clusters
            .iter()
            .position(|c| c.node_id() == victim)
            .expect("victim index");
        let old = clusters.remove(idx);
        old.shutdown_and_wait().await;
        let _ = net.detach(victim);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let cluster = CraftyCluster::builder(victim, Empty)
            .members(ids)
            .raft_config(raft_config(base_seed ^ victim.0 ^ rounds))
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20))
            .manage_auto::<SessionWorker>("w", 0)
            .start_local(&net)
            .await;
        clusters.insert(idx, Arc::new(cluster));
        restarts += 1;

        await_workers(&clusters, 3).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        casts_ok += cast_session_round(&clusters[0], &user, 4).await;
    }

    let handled = HANDLED.load(Ordering::Relaxed);
    for c in clusters {
        c.shutdown();
    }

    let secs = start.elapsed().as_secs_f64();
    println!(
        "soak_session OK: rounds={rounds} casts_ok={casts_ok} handled={handled} \
         restarts={restarts} in {secs:.1}s"
    );
    assert!(casts_ok > 0, "soak_session: expected successful session casts");
    assert!(handled > 0, "soak_session: expected worker deliveries");
    assert!(restarts > 0, "soak_session: expected at least one node restart");
}
