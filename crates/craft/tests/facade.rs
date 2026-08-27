//! End-to-end tests for the [`CraftCluster`] facade over the in-memory
//! `LocalNetwork`: a 3-node cluster elects a leader, serves proposals/queries
//! through the in-process handle (with transparent forwarding), auto-places a
//! managed worker group on every node via the leader-only supervisor, and
//! exposes live state through the admin/observability endpoints.

use std::sync::Arc;
use std::time::Duration;

use craft::actor::{ConfigCodecError, UserActor};
use craft::core::Config;
use craft::net::LocalNetwork;
use craft::proto;
use craft::{CraftCluster, NodeId};
use craft_test_support::{Cmd, Kv, Qry, Resp, TICK_PERIOD, fast_raft_config};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

/// Build a 3-node facade cluster on a fresh `LocalNetwork`, managing one
/// auto-worker group `"w"`.
async fn spawn_cluster() -> (LocalNetwork, Vec<Arc<CraftCluster<Kv>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(fast_raft_config())
            .tick_period(TICK_PERIOD)
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

/// Async variant of [`eventually`] for conditions that need a fresh await each tick.
async fn eventually_async<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..500 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

fn reachability_raft_config() -> Config {
    Config {
        election_timeout_min: 3,
        election_timeout_max: 5,
        heartbeat_interval: 1,
        seed: 7,
    }
}

/// 3-node cluster tuned for fast heartbeat-derived reachability (liveness-vs-membership).
async fn spawn_reachability_cluster() -> (LocalNetwork, Vec<Arc<CraftCluster<Kv>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(reachability_raft_config())
            .tick_period(Duration::from_millis(5))
            .refresh_period(Duration::from_millis(15))
            .reconcile_period(Duration::from_millis(15))
            .directory_publish_period(Duration::from_millis(20))
            .manage_auto::<Worker>("w", 0)
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }
    (net, clusters)
}

async fn wait_for_directory_workers(leader: &CraftCluster<Kv>, count: usize) {
    let directory = leader.directory().clone();
    eventually(
        &format!("{count} workers in the leader directory"),
        move || directory.lookup("w").len() == count,
    )
    .await;
}

async fn pick_follower(clusters: &[Arc<CraftCluster<Kv>>]) -> Arc<CraftCluster<Kv>> {
    for c in clusters {
        if !c.is_leader().await {
            return Arc::clone(c);
        }
    }
    panic!("no follower found");
}

async fn wait_for_workers_on_every_node(clusters: &[Arc<CraftCluster<Kv>>]) {
    for c in clusters {
        let reg = c.registry().clone();
        let id = c.node_id();
        eventually(&format!("worker on node {id:?}"), move || reg.contains("w")).await;
    }
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
    assert_eq!(resp, Resp::Set { previous: None });

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
    let leader = leader(&clusters).await;

    // Drive reconcile explicitly via the public supervisor accessor.
    let report = leader.supervisor().reconcile().await;
    assert!(report.is_ok(), "initial reconcile should succeed");

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
async fn follower_scale_cluster_forwards_to_leader() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();

    // A 3-node cluster with no managed groups: placement is driven purely by an
    // imperative `scale_cluster`. Every node registers the type so any of them
    // can host / reconstruct it.
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = Arc::new(
            CraftCluster::builder(id, Kv::default())
                .members(ids)
                .raft_config(fast_raft_config())
                .tick_period(Duration::from_millis(10))
                .reconcile_period(Duration::from_millis(20))
                .directory_publish_period(Duration::from_millis(20))
                .start_local(&net)
                .await,
        );
        cluster.control().register_type::<Worker>();
        clusters.push(cluster);
    }

    let leader = leader(&clusters).await;
    let follower = clusters
        .iter()
        .find(|c| c.node_id() != leader.node_id())
        .expect("a follower exists")
        .clone();

    // Scaling from a *follower* must transparently forward to the leader
    // (supervisor-leader), which plans and executes one worker per node.
    follower
        .scale_cluster::<Worker>("w", 3, 0)
        .await
        .expect("scale forwarded to leader and applied");

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
            .raft_config(fast_raft_config())
            .tick_period(TICK_PERIOD)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_publishes_consensus_and_actor_metrics() {
    let (_net, clusters) = spawn_cluster().await;
    let leader = leader(&clusters).await;

    // A worker is placed on the leader, and its lifecycle spawn bumps the
    // spawn counter; the consensus sampler publishes Raft gauges.
    let metrics = leader.metrics().clone();
    eventually("consensus + actor metrics on leader", move || {
        let out = metrics.render();
        out.contains("craft_raft_is_leader")
            && out.contains("craft_actor_spawns_total")
            && out.contains("craft_actor_instances{actor=\"w\"}")
    })
    .await;

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_tracing_emits_message_handled_events() {
    use craft::TraceOpts;

    let (_net, clusters) = spawn_cluster().await;
    let leader = leader(&clusters).await;

    // Wait for the worker group to be placed on the leader.
    let reg = leader.registry().clone();
    eventually("worker on leader", move || reg.contains("w")).await;

    // Subscribe, enable tracing for "w", then drive one message through it.
    let mut sub = leader.events().subscribe();
    leader.trace("w", TraceOpts::default());
    leader
        .registry()
        .pool::<Worker>("w")
        .expect("worker pool")
        .send(())
        .expect("cast to worker");

    // A MessageHandled event should surface for the traced group.
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sub.recv().await {
                Some(craft::CraftEvent::MessageHandled { id, .. }) if id.starts_with("w#") => {
                    break true;
                }
                Some(_) => continue,
                None => break false,
            }
        }
    })
    .await
    .expect("did not receive MessageHandled within timeout");
    assert!(got, "event bus closed before a MessageHandled arrived");

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test]
async fn builder_wires_actor_state_store() {
    use craft::actor::{ActorStateStore, InMemoryStore};

    let net = LocalNetwork::new();
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let cluster = CraftCluster::builder(NodeId(1), Kv::default())
        .actor_state_store(Arc::clone(&store))
        .start_local(&net)
        .await;

    let wired = cluster.actor_state_store().expect("store configured");
    assert!(Arc::ptr_eq(&store, &wired));
    cluster.shutdown();
}

#[tokio::test]
async fn builder_wires_resource_profile() {
    use craft::{ResourceProfile, VpsResources};

    let net = LocalNetwork::new();
    let cluster = CraftCluster::builder(NodeId(1), Kv::default())
        .resource_profile(ResourceProfile::Limited { worker_threads: 2 })
        .start_local(&net)
        .await;

    assert_eq!(
        cluster.resource_profile(),
        ResourceProfile::Limited { worker_threads: 2 }
    );
    assert_eq!(
        cluster.vps_resources(),
        VpsResources::from_parallelism(
            cluster.vps_resources().available_parallelism,
            ResourceProfile::Limited { worker_threads: 2 },
        )
    );
    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_driven_reconcile_reacts_to_reachability_not_membership() {
    let (net, clusters) = spawn_reachability_cluster().await;
    wait_for_workers_on_every_node(&clusters).await;

    let leader_node = leader(&clusters).await;
    leader_node.supervisor().reconcile().await;
    wait_for_directory_workers(&leader_node, 3).await;

    let victim = pick_follower(&clusters).await.node_id();
    assert!(net.detach(victim), "victim was attached");

    eventually_async(
        "leader marks the victim unreachable while it remains a voter",
        || async {
            let l = leader(&clusters).await;
            let Some(status) = l.status().await else {
                return false;
            };
            status.voters.contains(&victim) && !status.reachable.contains(&victim)
        },
    )
    .await;

    eventually_async("supervisor reconciles to two reachable workers", || async {
        let l = leader(&clusters).await;
        let report = l.supervisor().reconcile().await;
        report.is_ok() && report.groups[0].total == 2
    })
    .await;

    for c in clusters.iter().filter(|c| c.node_id() != victim) {
        let reg = c.registry().clone();
        assert!(
            reg.contains("w"),
            "survivor {:?} keeps its worker",
            c.node_id()
        );
    }

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healed_node_gets_auto_worker_respawned_after_partition() {
    let (net, clusters) = spawn_reachability_cluster().await;
    wait_for_workers_on_every_node(&clusters).await;

    let leader_node = leader(&clusters).await;
    leader_node.supervisor().reconcile().await;
    wait_for_directory_workers(&leader_node, 3).await;

    let healed = pick_follower(&clusters).await;
    let victim = healed.node_id();

    assert!(net.detach(victim));
    eventually_async("victim unreachable on leader", || async {
        let l = leader(&clusters).await;
        let Some(status) = l.status().await else {
            return false;
        };
        status.voters.contains(&victim) && !status.reachable.contains(&victim)
    })
    .await;

    net.attach(victim, healed.wire_handler());
    eventually_async(
        "victim reachable again without membership change",
        || async {
            let l = leader(&clusters).await;
            let Some(status) = l.status().await else {
                return false;
            };
            status.voters.contains(&victim) && status.reachable.contains(&victim)
        },
    )
    .await;

    eventually_async(
        "supervisor reconciles back to three reachable workers",
        || async {
            let l = leader(&clusters).await;
            let report = l.supervisor().reconcile().await;
            report.is_ok() && report.groups[0].total == 3
        },
    )
    .await;

    assert!(
        healed.registry().contains("w"),
        "worker present on the healed node"
    );

    for c in &clusters {
        c.shutdown();
    }
}
