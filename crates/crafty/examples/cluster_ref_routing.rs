//! Resolve cluster-wide actor pools via [`ClusterRef`] (cross-node-actors E7): pick a
//! target instance round-robin or by key, then cast locally or cross-node.
//!
//! Run with: `cargo run -p crafty --example cluster_ref_routing`

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crafty::actor::{UserActor, remote_actor};
use crafty::core::{Config, StateMachine};
use crafty::net::LocalNetwork;
use crafty::proto::LogIndex;
use crafty::{CraftyCluster, NodeId};

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

#[remote_actor]
impl UserActor for Worker {
    type Config = u32;
    type Message = u64;
    type Error = WorkerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    fn handle(
        &mut self,
        job: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        HANDLED.fetch_add(1, Ordering::SeqCst);
        println!("worker handled job {job}");
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

async fn eventually<F: FnMut() -> bool>(what: &str, mut cond: F) {
    for _ in 0..500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();

    for &id in &ids {
        let cluster = CraftyCluster::builder(id, Empty)
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
            .manage_auto::<Worker>("worker", 0)
            .start_local(&net)
            .await;
        clusters.push(cluster);
    }

    let leader = loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut found = None;
        for c in &clusters {
            if c.is_leader().await {
                found = Some(c);
                break;
            }
        }
        if let Some(c) = found {
            break c;
        }
    };

    let _ = leader.supervisor().reconcile().await;

    for c in &clusters {
        eventually("worker in directory", || {
            c.directory().lookup("worker").len() == ids.len()
        })
        .await;
    }

    // ClusterRef: a handle to the cluster-wide "worker" pool.
    let pool = leader.directory().cluster("worker");
    assert_eq!(pool.len(), ids.len());

    let target = pool.pick().expect("pool has members");
    println!(
        "ClusterRef picked {}#{} on node {}",
        target.id.name, target.id.instance, target.id.node.0
    );

    let payload = crafty::proto::encode(&42u64)?;
    leader.messaging().cast("worker", payload).await?;

    eventually("message handled", || HANDLED.load(Ordering::SeqCst) >= 1).await;

    let keyed = pool.pick_keyed(&"tenant-7").expect("keyed pick");
    println!("keyed pick stable: {}#{}", keyed.id.name, keyed.id.instance);

    for c in clusters {
        c.shutdown();
    }
    Ok(())
}
