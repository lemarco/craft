//! Hexagonal wiring: consensus port vs actor workflow store (architecture-style).
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────┐
//!   │  CraftyCluster (facade / runtime)                        │
//!   │    Raft StateMachine  ←── linearizable, replicated      │
//!   │    ActorStateStore    ←── workflow keys, survives crash │
//!   └─────────────────────────────────────────────────────────┘
//!          ▲                           ▲
//!          │ Command/Query             │ opaque bytes + store_get/store_set
//!          │ (serde/postcard)          │ (same codec, no extra trait)
//!   ┌──────┴──────┐             ┌──────┴──────────────────┐
//!   │  Kv SM      │             │  InMemoryStore (dev)    │
//!   │  (in Raft)  │             │  RedisStore (prod)      │
//!   └─────────────┘             └─────────────────────────┘
//! ```
//!
//! Run with: `cargo run -p crafty --example hexagonal_actor_store`

use std::sync::Arc;
use std::time::Duration;

use crafty::actor::{ActorRef, ActorStateStore, InMemoryStore, UserActor, store_get, store_set};
use crafty::core::StateMachine;
use crafty::net::LocalNetwork;
use crafty::proto::LogIndex;
use crafty::{CraftyCluster, NodeId};
use serde::{Deserialize, Serialize};

// --- Consensus port (Raft StateMachine) ------------------------------------

#[derive(Default)]
struct Counter(u64);

impl StateMachine for Counter {
    type Command = u64;
    type Query = ();
    type Response = u64;
    type Error = std::convert::Infallible;

    fn apply(&mut self, _: LogIndex, cmd: &u64) -> Result<u64, Self::Error> {
        self.0 += *cmd;
        Ok(self.0)
    }

    fn query(&self, (): &()) -> Result<u64, Self::Error> {
        Ok(self.0)
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = u64::from_le_bytes(bytes.try_into().unwrap());
        Ok(())
    }
}

// --- Actor workflow port (external store) ------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct JobProgress {
    attempts: u32,
}

#[derive(Clone)]
struct WorkerCfg {
    store: Arc<dyn ActorStateStore>,
}

struct JobWorker {
    store: Arc<dyn ActorStateStore>,
}

#[derive(Debug)]
struct WorkerErr;
impl std::fmt::Display for WorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerErr {}

impl UserActor for JobWorker {
    type Config = WorkerCfg;
    type Message = u64;
    type Error = WorkerErr;

    fn start(cfg: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self { store: cfg.store })
    }

    async fn handle(&mut self, job_id: Self::Message) -> Result<(), Self::Error> {
        let key = format!("job:{job_id}");
        let mut progress = store_get::<JobProgress>(&*self.store, &key)
            .await
            .map_err(|_| WorkerErr)?;
        if progress.as_ref().is_some_and(|p| p.attempts > 0) {
            println!("job {job_id}: already processed ({progress:?}), skipping");
            return Ok(());
        }
        progress = Some(JobProgress { attempts: 1 });
        store_set(&*self.store, &key, &progress.unwrap(), None)
            .await
            .map_err(|_| WorkerErr)?;
        println!("job {job_id}: processed");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let net = LocalNetwork::new();
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());

    let cluster = CraftyCluster::builder(NodeId(1), Counter::default())
        .actor_state_store(Arc::clone(&store))
        .register_actor::<JobWorker>()
        .tick_period(Duration::from_millis(10))
        .start_local(&net)
        .await;

    let store = cluster
        .actor_state_store()
        .expect("builder wired ActorStateStore");

    let worker: ActorRef<JobWorker> = cluster
        .registry()
        .spawn(
            "jobs",
            WorkerCfg {
                store: Arc::clone(&store),
            },
        )
        .expect("spawn worker");

    worker.send(42).expect("first delivery");
    worker.send(42).expect("redelivery is idempotent");

    cluster.shutdown();
}
