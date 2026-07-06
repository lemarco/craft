//! End-to-end tests for the [`CraftCluster`] facade over the in-memory
//! `LocalNetwork`: a 3-node cluster elects a leader, serves proposals/queries
//! through the in-process handle (with transparent forwarding), auto-places a
//! managed worker group on every node via the leader-only supervisor, and
//! exposes live state through the admin/observability endpoints.

use std::sync::Arc;
use std::time::Duration;

use craft::actor::{ConfigCodecError, UserActor};
use craft::core::{Config, StateMachine};
use craft::net::LocalNetwork;
use craft::proto::{self, LogIndex};
use craft::{CraftCluster, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// --- Reference KV state machine -------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Cmd {
    Set { key: String, value: String },
}

#[derive(Debug, Serialize, Deserialize)]
enum Qry {
    Get { key: String },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Resp {
    Set,
    Value(Option<String>),
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
    map: BTreeMap<String, String>,
}

impl StateMachine for Kv {
    type Command = Cmd;
    type Query = Qry;
    type Response = Resp;
    type Error = KvError;

    fn apply(&mut self, _index: LogIndex, command: &Cmd) -> Result<Resp, KvError> {
        match command {
            Cmd::Set { key, value } => {
                self.map.insert(key.clone(), value.clone());
                Ok(Resp::Set)
            }
        }
    }

    fn query(&self, query: &Qry) -> Result<Resp, KvError> {
        match query {
            Qry::Get { key } => Ok(Resp::Value(self.map.get(key).cloned())),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        proto::encode(&self.map).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        self.map = proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

// --- A managed auto-worker ------------------------------------------------

#[derive(Debug)]
struct WorkerErr;
impl std::fmt::Display for WorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerErr {}

struct Worker;

impl UserActor for Worker {
    type Config = u32;
    type Message = ();
    type Error = WorkerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }
}

// --- Harness --------------------------------------------------------------

fn raft_config() -> Config {
    Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed: 7,
    }
}

/// Build a 3-node facade cluster on a fresh `LocalNetwork`, managing one
/// auto-worker group `"w"`.
async fn spawn_cluster() -> (LocalNetwork, Vec<Arc<CraftCluster<Kv>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(raft_config())
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20))
            .manage_auto::<Worker>("w", 0)
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    (net, clusters)
}

/// Poll until `cond` holds (checked every 10ms), or panic after ~5s.
async fn eventually<F>(what: &str, mut cond: F)
where
    F: FnMut() -> bool,
{
    for _ in 0..500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

async fn leader(clusters: &[Arc<CraftCluster<Kv>>]) -> Arc<CraftCluster<Kv>> {
    for _ in 0..500 {
        for c in clusters {
            if c.is_leader().await {
                return Arc::clone(c);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected");
}

/// Minimal blocking-free HTTP/1.1 GET returning `(status_code, body)`.
async fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect admin");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("send req");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read resp");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// Grab a currently-free localhost port (best-effort; used to bind the admin
/// server to a knowable address).
fn free_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

// --- Tests ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_elects_leader_and_serves_reads_and_writes() {
    let (_net, clusters) = spawn_cluster().await;
    let leader = leader(&clusters).await;

    let resp = leader
        .handle()
        .propose(Cmd::Set {
            key: "a".into(),
            value: "1".into(),
        })
        .await
        .expect("propose on leader");
    assert_eq!(resp, Resp::Set);

    let resp = leader
        .handle()
        .query(Qry::Get { key: "a".into() })
        .await
        .expect("query on leader");
    assert_eq!(resp, Resp::Value(Some("1".into())));

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_auto_places_a_worker_on_every_node() {
    let (_net, clusters) = spawn_cluster().await;
    let _leader = leader(&clusters).await;

    // The leader's reconcile loop should place one "w" worker per live node.
    for c in &clusters {
        let reg = c.registry().clone();
        eventually(&format!("worker on node {:?}", c.node_id()), move || {
            reg.contains("w")
        })
        .await;
    }

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_endpoints_report_live_state() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let admin_addr = free_port();

    let mut clusters = Vec::new();
    for &id in &ids {
        let mut builder = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(raft_config())
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20))
            .manage_auto::<Worker>("w", 0);
        // Only node 1 serves the admin port in this test.
        if id == NodeId(1) {
            builder = builder.admin_addr(admin_addr);
        }
        clusters.push(Arc::new(builder.start_local(&net).await));
    }

    let _leader = leader(&clusters).await;

    // Health is OK once the server is up (bind is awaited during start_local,
    // but the accept loop is spawned; retry briefly).
    let mut health = (0, String::new());
    for _ in 0..200 {
        health = http_get(admin_addr, "/health").await;
        if health.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(health.0, 200, "health body: {}", health.1);

    // Cluster view eventually shows a leader and all three voters.
    let mut cluster_body = String::new();
    for _ in 0..500 {
        let (s, b) = http_get(admin_addr, "/introspect/cluster").await;
        if s == 200 && b.contains("\"leader\"") && b.contains("\"id\":3") {
            cluster_body = b;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        cluster_body.contains("\"id\":1")
            && cluster_body.contains("\"id\":2")
            && cluster_body.contains("\"id\":3"),
        "cluster view missing voters: {cluster_body}"
    );

    // Actors show up in introspection once the directory has published.
    let mut actors_body = String::new();
    for _ in 0..500 {
        let (s, b) = http_get(admin_addr, "/introspect/actors").await;
        if s == 200 && b.contains("w#") {
            actors_body = b;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        actors_body.contains("Worker"),
        "actors view missing worker type: {actors_body}"
    );

    // Metrics endpoint renders Prometheus text (may be empty families).
    let (status, _) = http_get(admin_addr, "/metrics").await;
    assert_eq!(status, 200);

    for c in &clusters {
        c.shutdown();
    }
}
