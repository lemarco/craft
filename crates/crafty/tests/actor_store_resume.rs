//! actor-state-redis / liveness-vs-membership: actor workflow state in a shared external store survives
//! a host becoming unreachable; a survivor's worker reads the same keys and
//! redeliveries stay idempotent. Uses `InMemoryStore` + `LocalNetwork` (fast lane).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use crafty::actor::{ActorStateStore, ConfigCodecError, InMemoryStore, UserActor};
use crafty::core::{Config, StateMachine};
use crafty::net::LocalNetwork;
use crafty::proto::{self, LogIndex};
use crafty::advanced::{CraftyCluster, NodeId};
use crafty_test_support::{await_crafty_leader, eventually_async_default};
use serde::{Deserialize, Serialize};

// --- Minimal KV state machine (cluster consensus only) ----------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Cmd {
    Ping,
}

#[derive(Debug, Serialize, Deserialize)]
enum Qry {
    Ping,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Resp {
    Pong,
}

#[derive(Debug)]
struct KvError;
impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("kv error")
    }
}
impl std::error::Error for KvError {}

#[derive(Default)]
struct Kv {
    _pad: BTreeMap<String, String>,
}

impl StateMachine for Kv {
    type Command = Cmd;
    type Query = Qry;
    type Response = Resp;
    type Error = KvError;

    fn apply(&mut self, _index: LogIndex, _command: &Cmd) -> Result<Resp, KvError> {
        Ok(Resp::Pong)
    }

    fn query(&self, _query: &Qry) -> Result<Resp, KvError> {
        Ok(Resp::Pong)
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), KvError> {
        Ok(())
    }
}

// --- Order worker backed by the shared external store -----------------------

#[derive(Clone)]
struct Fixture {
    store: Arc<dyn ActorStateStore>,
    effects: Arc<AtomicU32>,
}

static FIXTURES: LazyLock<Mutex<HashMap<u64, Fixture>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

fn register_fixture(store: Arc<dyn ActorStateStore>) -> (u64, Arc<AtomicU32>) {
    let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let effects = Arc::new(AtomicU32::new(0));
    FIXTURES.lock().expect("fixtures").insert(
        id,
        Fixture {
            store,
            effects: Arc::clone(&effects),
        },
    );
    (id, effects)
}

#[derive(Clone, Copy)]
struct OrderWorkerCfg(u64);

#[derive(Debug)]
struct OrderWorkerErr;
impl std::fmt::Display for OrderWorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("order worker error")
    }
}
impl std::error::Error for OrderWorkerErr {}

struct OrderWorker {
    store: Arc<dyn ActorStateStore>,
    effects: Arc<AtomicU32>,
}

impl UserActor for OrderWorker {
    type Config = OrderWorkerCfg;
    type Message = u64;
    type Error = OrderWorkerErr;

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        proto::encode(&config.0).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        let id: u64 = proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))?;
        Ok(OrderWorkerCfg(id))
    }

    fn start(cfg: Self::Config) -> Result<Self, Self::Error> {
        let fixture = FIXTURES
            .lock()
            .expect("fixtures")
            .get(&cfg.0)
            .cloned()
            .ok_or(OrderWorkerErr)?;
        Ok(Self {
            store: fixture.store,
            effects: fixture.effects,
        })
    }

    async fn handle(&mut self, order_id: Self::Message) -> Result<(), Self::Error> {
        let key = format!("order:{order_id}");
        let claimed = self
            .store
            .compare_and_set(&key, None, b"processing", None)
            .await
            .map_err(|_| OrderWorkerErr)?;
        if !claimed {
            return Ok(());
        }
        self.effects.fetch_add(1, Ordering::SeqCst);
        self.store
            .set(&key, b"done", None)
            .await
            .map_err(|_| OrderWorkerErr)?;
        Ok(())
    }
}

// --- Harness ----------------------------------------------------------------

fn reachability_raft_config() -> Config {
    Config {
        election_timeout_min: 3,
        election_timeout_max: 5,
        heartbeat_interval: 1,
        seed: 21,
        ..Default::default()
    }
}

async fn spawn_store_cluster(
    store: Arc<dyn ActorStateStore>,
    cfg_id: u64,
) -> (LocalNetwork, Vec<Arc<CraftyCluster<Kv>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(reachability_raft_config())
            .tick_period(Duration::from_millis(5))
            .refresh_period(Duration::from_millis(15))
            .reconcile_period(Duration::from_millis(15))
            .directory_publish_period(Duration::from_millis(20))
            .actor_state_store(Arc::clone(&store))
            .manage_auto::<OrderWorker>("orders", OrderWorkerCfg(cfg_id))
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    (net, clusters)
}

async fn wait_for_workers_on_every_node(clusters: &[Arc<CraftyCluster<Kv>>]) {
    for c in clusters {
        let reg = c.registry().clone();
        let id = c.node_id();
        eventually_async_default(&format!("order worker on node {id:?}"), || {
            let reg = reg.clone();
            async move { reg.contains("orders") }
        })
        .await;
    }
}

fn cluster_by_id(clusters: &[Arc<CraftyCluster<Kv>>], id: NodeId) -> Arc<CraftyCluster<Kv>> {
    Arc::clone(
        clusters
            .iter()
            .find(|c| c.node_id() == id)
            .unwrap_or_else(|| panic!("node {id:?} missing")),
    )
}

async fn wait_for_effects(effects: Arc<AtomicU32>, want: u32) {
    eventually_async_default(&format!("side effects == {want}"), || {
        let effects = Arc::clone(&effects);
        async move { effects.load(Ordering::SeqCst) == want }
    })
    .await;
}

async fn wait_for_order_done(store: &dyn ActorStateStore, order_id: u64) {
    let key = format!("order:{order_id}");
    eventually_async_default(&format!("{key} marked done"), || async {
        matches!(
            store.get(&key).await.ok().flatten().as_deref(),
            Some(b"done")
        )
    })
    .await;
}

// --- Tests ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn actor_store_redelivery_is_idempotent_on_one_node() {
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let (cfg_id, effects) = register_fixture(Arc::clone(&store));
    let (_net, clusters) = spawn_store_cluster(Arc::clone(&store), cfg_id).await;
    wait_for_workers_on_every_node(&clusters).await;

    let worker = cluster_by_id(&clusters, NodeId(2))
        .registry()
        .get::<OrderWorker>("orders")
        .expect("worker on node 2");
    for _ in 0..3 {
        worker.send(42).expect("deliver order 42");
    }

    wait_for_effects(Arc::clone(&effects), 1).await;
    wait_for_order_done(store.as_ref(), 42).await;

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn actor_store_survives_unreachable_node_and_resumes_on_survivor() {
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let (cfg_id, effects) = register_fixture(Arc::clone(&store));
    let (net, clusters) = spawn_store_cluster(Arc::clone(&store), cfg_id).await;
    wait_for_workers_on_every_node(&clusters).await;

    let victim = NodeId(2);
    let survivor = NodeId(1);

    let victim_worker = cluster_by_id(&clusters, victim)
        .registry()
        .get::<OrderWorker>("orders")
        .expect("worker on victim");
    victim_worker.send(42).expect("process on victim");
    wait_for_effects(Arc::clone(&effects), 1).await;
    wait_for_order_done(store.as_ref(), 42).await;

    assert!(net.detach(victim), "victim was attached");
    eventually_async_default("leader marks victim unreachable", || async {
        let l = await_crafty_leader(&clusters).await;
        let Some(status) = l.status().await else {
            return false;
        };
        status.voters.contains(&victim) && !status.reachable.contains(&victim)
    })
    .await;

    let survivor_worker = cluster_by_id(&clusters, survivor)
        .registry()
        .get::<OrderWorker>("orders")
        .expect("worker on survivor");
    for _ in 0..2 {
        survivor_worker.send(42).expect("redeliver after partition");
    }
    wait_for_effects(Arc::clone(&effects), 1).await;

    survivor_worker.send(43).expect("new order on survivor");
    wait_for_effects(Arc::clone(&effects), 2).await;
    wait_for_order_done(store.as_ref(), 43).await;

    let wired = cluster_by_id(&clusters, survivor)
        .actor_state_store()
        .expect("builder wired store");
    assert!(
        Arc::ptr_eq(&store, &wired),
        "facade exposes the same shared store handle"
    );

    for c in &clusters {
        c.shutdown();
    }
}
