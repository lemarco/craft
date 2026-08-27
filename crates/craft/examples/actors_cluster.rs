//! Cluster actors end-to-end: a **managed auto-worker** group placed one-per-node
//! by the leader (auto-spawn-on-join), then messages cast round-robin across the whole
//! cluster (cluster-routing) — some delivered locally, some shipped to a peer node over
//! the actor wire (cross-node-actors). The `#[remote_actor]` attribute generates the wire
//! codecs that make `Worker` remotely addressable.
//!
//! Run with: `cargo run -p craft --example actors_cluster`

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use craft::actor::{UserActor, remote_actor};
use craft::core::{Config, StateMachine};
use craft::net::LocalNetwork;
use craft::proto::LogIndex;
use craft::{CraftCluster, NodeId};

/// Counts messages handled across *all* workers in this process, to prove casts
/// were delivered (locally and cross-node).
static HANDLED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct WorkerErr;
impl std::fmt::Display for WorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerErr {}

struct Worker;

// The attribute fills in the postcard config/message codecs, making `Worker`
// spawnable and addressable across nodes.
#[remote_actor]
impl UserActor for Worker {
    type Config = u32;
    type Message = u64;
    type Error = WorkerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    async fn handle(&mut self, job: Self::Message) -> Result<(), Self::Error> {
        HANDLED.fetch_add(1, Ordering::SeqCst);
        println!("worker handled job {job}");
        Ok(())
    }
}

// A trivial no-op state machine — this example is about actors, not consensus
// data, but every cluster still replicates one.
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

/// Poll `cond` (every 10ms) until true, or panic after ~5s.
async fn eventually<F: FnMut() -> bool>(what: &str, mut cond: F) {
    for _ in 0..500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();

    let mut clusters = Vec::new();
    for &id in &ids {
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
            .directory_publish_period(Duration::from_millis(20))
            // One `Worker` on every live node, kept placed by the leader.
            .manage_auto::<Worker>("w", 0)
            .start_local(&net)
            .await;
        clusters.push(cluster);
    }

    // The leader's reconcile loop places a worker on each node…
    for c in &clusters {
        let reg = c.registry().clone();
        eventually("worker placed on every node", move || reg.contains("w")).await;
    }
    // …and every node's directory converges on all three instances.
    let dir = clusters[0].directory().clone();
    eventually("directory sees 3 workers", move || {
        dir.lookup("w").len() == 3
    })
    .await;
    println!("workers placed on nodes {ids:?}");

    // Cast 6 jobs round-robin across the cluster from node 1; roughly two land
    // on each node, exercising both local and cross-node delivery.
    let jobs = 6u64;
    for job in 0..jobs {
        let payload = craft::proto::encode(&job)?;
        clusters[0].messaging().cast("w", payload).await?;
    }

    eventually("all jobs handled", || {
        HANDLED.load(Ordering::SeqCst) == jobs
    })
    .await;
    println!("all {jobs} jobs handled across the cluster ✓");

    for c in &clusters {
        c.shutdown();
    }
    Ok(())
}
