//! Cluster job queue worker — enqueue on the leader, **lease/ack on a follower**
//! via [`ClusterJobQueue`], then survive **leader failover**
//! ([job-queue](../../docs/decisions/job-queue.md)).
//!
//! For sharded streams, priority, dedup, and autoscale see `job_queue_cluster`.
//!
//! Run: `cargo run -p craft --example job_queue_worker`

use std::sync::Arc;
use std::time::Duration;

use craft::core::{Config, StateMachine};
use craft::net::LocalNetwork;
use craft::proto::LogIndex;
use craft::{CraftCluster, NodeId, WorkerId, run_queue_consumer};

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

async fn await_leader(clusters: &[Arc<CraftCluster<Empty>>]) -> NodeId {
    for _ in 0..500 {
        for c in clusters {
            if c.is_leader().await {
                return c.node_id();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected");
}

#[tokio::main]
#[allow(clippy::too_many_lines)] // end-to-end job queue worker demo
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::temp_dir().join("craft-job-queue-worker-example");
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
                seed: 7,
                ..Default::default()
            })
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .data_dir(&data_dir)
            .job_queue("jobs", Duration::from_secs(60))
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    let leader_id = await_leader(&clusters).await;
    println!("leader elected: {leader_id:?}");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let leader = clusters
        .iter()
        .find(|c| c.node_id() == leader_id)
        .expect("leader cluster");
    let submit_queue = leader.job_queue("jobs").expect("queue on leader");

    for i in 0..5u64 {
        let payload = format!("job-{i}");
        let id = submit_queue.enqueue(payload.as_bytes()).await?;
        println!("enqueued {id:?} on leader");
    }

    let follower = clusters
        .iter()
        .find(|c| c.node_id() != leader_id)
        .expect("follower");
    let follower_id = follower.node_id();
    let worker_queue = follower.job_queue("jobs").expect("queue on follower");
    println!("worker runs on follower node {follower_id:?} via ClusterJobQueue");

    let worker_id = WorkerId {
        node: follower_id,
        instance: 1,
    };
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = tokio::spawn(async move {
        run_queue_consumer(
            worker_queue,
            worker_id,
            2,
            Duration::from_millis(50),
            stop_rx,
            |payload| {
                let bytes = payload.to_vec();
                async move {
                    let text = String::from_utf8_lossy(&bytes);
                    println!("follower worker handled {text}");
                    Ok::<(), ()>(())
                }
            },
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(800)).await;
    stop_tx.send(true)?;
    consumer.await?;

    let metrics = submit_queue.metrics().await?;
    println!(
        "after follower worker: pending={} leased={}",
        metrics.pending, metrics.leased
    );

    // Kill the leader; survivors elect a new one with the replicated backlog.
    let old_leader = Arc::clone(leader);
    let old_leader_id = old_leader.node_id();
    old_leader.shutdown();
    let _ = net.detach(old_leader_id);
    drop(old_leader);

    let survivors: Vec<_> = clusters
        .iter()
        .filter(|c| c.node_id() != old_leader_id)
        .cloned()
        .collect();
    let new_leader_id = await_leader(&survivors).await;
    println!("failover: new leader {new_leader_id:?} (was {old_leader_id:?})");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let new_leader = survivors
        .iter()
        .find(|c| c.node_id() == new_leader_id)
        .expect("new leader");
    let queue = new_leader.job_queue("jobs").expect("queue after failover");
    let pending = queue.metrics().await?.pending;
    println!("backlog after failover: pending={pending}");
    if pending != 3 {
        return Err(format!("expected pending=3 after partial consume, got {pending}").into());
    }

    let worker = WorkerId {
        node: new_leader_id,
        instance: 2,
    };
    let remaining = queue.lease(worker, 5).await?;
    if remaining.len() != 3 {
        return Err(format!("expected 3 jobs to lease, got {}", remaining.len()).into());
    }
    for job in &remaining {
        queue.ack(worker, job.lease_id).await?;
        println!("post-failover ack {:?}", job.job_id);
    }

    if queue.metrics().await?.pending != 0 {
        return Err("expected empty queue after drain".into());
    }

    for c in clusters {
        if c.node_id() != old_leader_id {
            c.shutdown();
        }
    }
    Ok(())
}
