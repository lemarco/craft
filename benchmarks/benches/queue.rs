//! `queue` — job queue enqueue throughput (backlog T10, job-queue ADR hotspot).
//!
//! Compares local backends ([`InMemoryJobQueue`], [`RedbJobQueue`]) against the
//! full 3-node cluster path: leader append + synchronous voter replicate over
//! [`LocalNetwork`].

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use craft::core::{Config, StateMachine};
use craft::net::LocalNetwork;
use craft::proto::LogIndex;
use craft::{CraftCluster, JobQueue, NodeId};
use craft_actor::{InMemoryJobQueue, RedbJobQueue, WorkerId};
use craft_benchmarks::{env_u64, queue_payload};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};

const STREAM: &str = "bench-jobs";
const PAYLOAD: usize = 256;

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

struct ClusterBench {
    _base: tempfile::TempDir,
    #[allow(dead_code)]
    clusters: Vec<Arc<CraftCluster<Empty>>>,
    submit: Arc<dyn JobQueue>,
    drain: Arc<dyn JobQueue>,
    worker: WorkerId,
}

async fn await_leader(clusters: &[Arc<CraftCluster<Empty>>]) {
    for _ in 0..500 {
        for c in clusters {
            if c.is_leader().await {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("cluster bench: no leader elected");
}

async fn setup_cluster() -> ClusterBench {
    let base = tempfile::tempdir().expect("tempdir");
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();

    for &id in &ids {
        let data_dir = base.path().join(format!("node-{}", id.0));
        std::fs::create_dir_all(&data_dir).expect("mkdir data_dir");
        let cluster = CraftCluster::builder(id, Empty)
            .members(ids)
            .raft_config(raft_config(0x51_0AD ^ id.0))
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .data_dir(&data_dir)
            .job_queue(STREAM, Duration::from_secs(60))
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    await_leader(&clusters).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let submit = clusters[0]
        .job_queue(STREAM)
        .expect("queue client on node1");
    let drain = clusters[1]
        .job_queue(STREAM)
        .expect("queue client on node2");

    ClusterBench {
        _base: base,
        clusters,
        submit,
        drain,
        worker: WorkerId {
            node: NodeId(2),
            instance: 1,
        },
    }
}

async fn drain_batch(bench: &ClusterBench, max: usize) {
    if let Ok(jobs) = bench.drain.lease(bench.worker, max).await {
        for job in jobs {
            let _ = bench.drain.ack(bench.worker, job.lease_id).await;
        }
    }
}

fn bench_queue(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let lease = Duration::from_secs(60);
    let payload_size = env_u64("BENCH_QUEUE_PAYLOAD", PAYLOAD as u64) as usize;

    let mut group = c.benchmark_group("queue");
    group.throughput(Throughput::Bytes(payload_size as u64));

    group.bench_function("in_memory/enqueue", |b| {
        b.to_async(&rt).iter_batched(
            || InMemoryJobQueue::new(lease),
            |queue| async move {
                black_box(
                    queue
                        .enqueue(black_box(&queue_payload(payload_size, 1)))
                        .await
                        .expect("enqueue"),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("redb/enqueue", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("queue.redb");
                let queue = RedbJobQueue::open(path, lease).expect("open redb queue");
                (dir, queue)
            },
            |(_dir, queue)| async move {
                black_box(
                    queue
                        .enqueue(black_box(&queue_payload(payload_size, 1)))
                        .await
                        .expect("enqueue"),
                );
            },
            BatchSize::SmallInput,
        );
    });

    let cluster = rt.block_on(setup_cluster());
    let mut seq = 0u64;
    group.bench_function("cluster_3node/enqueue_replicated", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let bench = &cluster;
            async move {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    seq = seq.wrapping_add(1);
                    let payload = queue_payload(payload_size, seq);
                    bench
                        .submit
                        .enqueue(black_box(&payload))
                        .await
                        .expect("cluster enqueue");
                    if seq.is_multiple_of(32) {
                        drain_batch(bench, 32).await;
                    }
                }
                start.elapsed()
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_queue);
criterion_main!(benches);
