//! Three-node cluster with a durable job queue: enqueue via any node, consume
//! on a **follower** through [`ClusterJobQueue`]
//! ([job-queue](../../docs/decisions/job-queue.md)).
//!
//! Demonstrates v2 features: sharded streams, priority/delayed enqueue, dedup keys,
//! worker autoscale, and an optional membership-join hook. For a minimal follower
//! worker loop + leader failover, see `job_queue_worker`.
//!
//! Run: `cargo run -p craft --example job_queue_cluster`

use std::sync::Arc;
use std::time::Duration;

use craft::actor::{UserActor, remote_actor};
use craft::core::{Config, StateMachine};
use craft::net::LocalNetwork;
use craft::proto::LogIndex;
use craft::{
    AutoscalePolicy, CraftCluster, EnqueueOptions, MembershipAutoscalePolicy, NodeId, WorkerId,
    run_queue_consumer,
};

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

#[derive(Debug)]
struct WorkerErr;
impl std::fmt::Display for WorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerErr {}

struct Worker;

#[remote_actor]
impl UserActor for Worker {
    type Config = u32;
    type Message = ();
    type Error = WorkerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    fn handle(
        &mut self,
        _msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)] // end-to-end sharded job queue cluster demo
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::temp_dir().join("craft-job-queue-cluster-example");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base)?;

    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();

    for &id in &ids {
        let data_dir = base.join(format!("node-{}", id.0));
        std::fs::create_dir_all(&data_dir)?;
        let cluster = CraftCluster::builder(id, Empty)
            .members(ids)
            .raft_config(Config {
                election_timeout_min: 5,
                election_timeout_max: 10,
                heartbeat_interval: 2,
                seed: 9,
                ..Default::default()
            })
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .data_dir(&data_dir)
            .job_queue_sharded("jobs", 2, Duration::from_secs(60))
            .manage::<Worker>("workers", 1, 0)
            .job_queue_autoscale::<Worker>(
                "jobs",
                &AutoscalePolicy {
                    worker_group: "workers".into(),
                    target_pending_per_worker: 2,
                    min_workers: 1,
                    max_workers: 3,
                    cooldown: Duration::from_secs(1),
                    poll_interval: Duration::from_millis(100),
                },
                0,
            )
            .job_queue_membership_autoscale(
                "jobs",
                &MembershipAutoscalePolicy {
                    pending_per_node_threshold: 100,
                    max_nodes: 3,
                    cooldown: Duration::from_secs(30),
                    poll_interval: Duration::from_secs(5),
                },
                || {
                    Box::pin(async {
                        println!("membership autoscale: deploy another VPS and JOIN_ADDR here");
                        Ok(())
                    })
                },
            )
            .start_local(&net)
            .await;
        clusters.push(cluster);
    }

    let mut leader = None;
    for _ in 0..500 {
        for c in &clusters {
            if c.is_leader().await {
                leader = Some(c.node_id());
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let leader_id = leader.expect("a leader");
    println!("leader elected: {leader_id:?}");

    let submitter = clusters
        .iter()
        .find_map(|c| c.job_queue("jobs"))
        .expect("queue wired");

    let follower = clusters
        .iter()
        .find(|c| c.node_id() != leader_id)
        .expect("follower");
    let consumer_queue = follower.job_queue("jobs").expect("follower queue");
    let worker_node = follower.node_id();
    println!("consumer on follower node {worker_node:?}");

    let urgent = submitter
        .enqueue_opts(b"urgent", EnqueueOptions::priority(10))
        .await?;
    println!("enqueued priority job {urgent:?}");

    let idempotent = submitter
        .enqueue_opts(b"pay", EnqueueOptions::dedup_key("invoice-7"))
        .await?;
    let retry = submitter
        .enqueue_opts(b"pay-retry", EnqueueOptions::dedup_key("invoice-7"))
        .await?;
    println!("dedup enqueue: first={idempotent:?} retry={retry:?} (same id expected)");

    let delayed = submitter
        .enqueue_opts(b"later", EnqueueOptions::delayed(Duration::from_secs(2)))
        .await?;
    println!("delayed job {delayed:?} (visible after 2s)");

    for i in 0..6u64 {
        let payload = format!("job-{i}");
        let id = submitter.enqueue(payload.as_bytes()).await?;
        println!("enqueued {id:?}");
    }

    let consumer_queue = Arc::clone(&consumer_queue);
    let worker_id = WorkerId {
        node: worker_node,
        instance: 99,
    };
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = tokio::spawn(async move {
        run_queue_consumer(
            consumer_queue,
            worker_id,
            2,
            Duration::from_millis(50),
            stop_rx,
            |payload| {
                let bytes = payload.to_vec();
                async move {
                    let text = String::from_utf8_lossy(&bytes);
                    println!("handled {text}");
                    Ok::<(), ()>(())
                }
            },
        )
        .await;
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    stop_tx.send(true)?;
    consumer.await?;

    let metrics = submitter.metrics().await?;
    println!(
        "done: pending={} leased={} (workers autoscale up to node count; shards=2)",
        metrics.pending, metrics.leased
    );

    for c in clusters {
        c.shutdown();
    }
    Ok(())
}
