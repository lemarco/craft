//! Job queue soak: sustained enqueue + follower lease/ack over a 3-node cluster
//! (testing-strategy, job-queue ADR enqueue hotspot).
//!
//! Configure via env:
//!   SOAK_QUEUE_SECS       wall-clock budget (default 15)
//!   SOAK_QUEUE_SEED       payload / RNG base (default 0x510AD)
//!   SOAK_QUEUE_PAYLOAD    bytes per job (default 256)
//!   SOAK_QUEUE_DRAIN      lease/ack consumer on node 2 (default 1)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crafty::core::{Config, StateMachine};
use crafty::net::LocalNetwork;
use crafty::proto::LogIndex;
use crafty::{CraftyCluster, NodeId};
use crafty_actor::{WorkerId};
use crafty_benchmarks::{env_u64, queue_payload};

const STREAM: &str = "soak-jobs";

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

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key).ok().as_deref() {
        Some("0" | "false" | "FALSE" | "no" | "off") => false,
        Some("1" | "true" | "TRUE" | "yes" | "on") => true,
        Some(_) => true,
        None => default,
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

async fn await_leader(clusters: &[Arc<CraftyCluster<Empty>>]) -> NodeId {
    for _ in 0..500 {
        for c in clusters {
            if c.is_leader().await {
                return c.node_id();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("soak_queue: no leader elected");
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let budget = Duration::from_secs(env_u64("SOAK_QUEUE_SECS", 15));
    let base_seed = env_u64("SOAK_QUEUE_SEED", 0x51_0AD);
    let payload_size = env_u64("SOAK_QUEUE_PAYLOAD", 256) as usize;
    let drain_enabled = env_bool("SOAK_QUEUE_DRAIN", true);

    println!(
        "soak_queue: {budget:?} budget payload={payload_size}B drain={drain_enabled} (seed {base_seed:#x})"
    );

    let base = tempfile::tempdir().expect("tempdir");
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();

    for &id in &ids {
        let data_dir = base.path().join(format!("node-{}", id.0));
        std::fs::create_dir_all(&data_dir).expect("mkdir data_dir");
        let cluster = CraftyCluster::builder(id, Empty)
            .members(ids)
            .raft_config(raft_config(base_seed ^ id.0))
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .data_dir(&data_dir)
            .job_queue(STREAM, Duration::from_secs(60))
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    let leader = await_leader(&clusters).await;
    println!("soak_queue: leader = {leader:?}");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let submit = clusters[0]
        .job_queue(STREAM)
        .expect("submit queue on node1");
    let consumer = clusters[1]
        .job_queue(STREAM)
        .expect("consumer queue on node2");
    let worker = WorkerId {
        node: NodeId(2),
        instance: 1,
    };

    let running = Arc::new(AtomicBool::new(true));
    let drained = Arc::new(AtomicU64::new(0));
    let max_pending = Arc::new(AtomicU64::new(0));

    if drain_enabled {
        let consumer = Arc::clone(&consumer);
        let running = Arc::clone(&running);
        let drained = Arc::clone(&drained);
        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok(jobs) = consumer.lease(worker, 64).await {
                    for job in jobs {
                        if consumer.ack(worker, job.lease_id).await.is_ok() {
                            drained.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    let start = Instant::now();
    let mut seq = 0u64;
    let mut enqueued = 0u64;
    let mut enqueue_errors = 0u64;
    let mut last_metrics = Instant::now();

    while start.elapsed() < budget {
        seq = seq.wrapping_add(1);
        let payload = queue_payload(payload_size, base_seed ^ seq);
        match submit.enqueue(&payload).await {
            Ok(_) => enqueued += 1,
            Err(_) => enqueue_errors += 1,
        }

        if last_metrics.elapsed() >= Duration::from_secs(1) {
            if let Ok(m) = submit.metrics().await {
                max_pending.fetch_max(m.pending, Ordering::Relaxed);
            }
            last_metrics = Instant::now();
        }
    }

    running.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let secs = start.elapsed().as_secs_f64();
    let drained = drained.load(Ordering::Relaxed);
    let max_pending = max_pending.load(Ordering::Relaxed);
    let final_pending = submit.metrics().await.map(|m| m.pending).unwrap_or(0);

    for c in clusters {
        c.shutdown();
    }

    println!(
        "soak_queue OK: enqueued={enqueued} drained={drained} errors={enqueue_errors} \
         max_pending={max_pending} final_pending={final_pending} in {secs:.1}s \
         ({:.0} enqueue/s, {:.0} drain/s)",
        enqueued as f64 / secs,
        drained as f64 / secs
    );

    assert!(enqueued > 0, "soak_queue: expected at least one successful enqueue");
    if drain_enabled {
        assert!(drained > 0, "soak_queue: expected consumer to ack at least one job");
    }
}
