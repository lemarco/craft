//! End-to-end test of the **live QUIC + mTLS** transport via
//! [`CraftClusterBuilder::start_quic`]: a real 3-node cluster, each on its own
//! UDP socket, mutually authenticated by a shared dev cluster CA, elects a
//! leader and replicates proposals/queries over HTTP/3.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use craft::core::{Config, StateMachine};
use craft::net::tls::ClusterCa;
use craft::proto::{self, LogIndex};
use craft::{CraftCluster, NodeId, PeerDirectory, Security};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

// --- Harness --------------------------------------------------------------

fn raft_config() -> Config {
    Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed: 7,
    }
}

/// Grab a currently-free localhost UDP address for a QUIC listener.
fn free_udp() -> SocketAddr {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.local_addr().unwrap()
}

async fn await_leader(clusters: &[Arc<CraftCluster<Kv>>]) -> Arc<CraftCluster<Kv>> {
    for _ in 0..1000 {
        for c in clusters {
            if c.is_leader().await {
                return Arc::clone(c);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected over QUIC");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_cluster_elects_leader_and_replicates() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let addrs: Vec<SocketAddr> = ids.iter().map(|_| free_udp()).collect();

    // One shared dev CA so every node trusts every peer (mTLS).
    let ca = ClusterCa::generate().expect("dev CA");
    let peers: PeerDirectory = ids.iter().copied().zip(addrs.iter().copied()).collect();

    let mut clusters = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        // `Security::new` is always available; the dev CA supplies the material
        // (equivalent to the feature-gated `Security::dev` helper).
        let security = Security::new(
            ca.issue_node(id).expect("issue node cert"),
            ca.root_store().expect("trust root"),
        );
        let cluster = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(raft_config())
            .tick_period(Duration::from_millis(10))
            .start_quic(security, addrs[i], peers.clone())
            .await
            .expect("start quic node");
        clusters.push(Arc::new(cluster));
    }

    let leader = await_leader(&clusters).await;

    // Write through the leader and read it back linearizably.
    let resp = leader
        .handle()
        .propose(Cmd::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect("propose over quic");
    assert_eq!(resp, Resp::Set);

    let resp = leader
        .handle()
        .query(Qry::Get { key: "k".into() })
        .await
        .expect("query over quic");
    assert_eq!(resp, Resp::Value(Some("v".into())));

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_new_node_dynamically_joins_over_quic() {
    // Backlog E5 / ADR 007: a fourth node joins a live 3-node QUIC cluster
    // knowing ONLY the seed's address. It fetches the peer-address book from the
    // seed, is added by the leader via a membership change, and both directions
    // learn each other's addresses over `/cluster/peers`.
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let addrs: Vec<SocketAddr> = ids.iter().map(|_| free_udp()).collect();
    let ca = ClusterCa::generate().expect("dev CA");
    let peers: PeerDirectory = ids.iter().copied().zip(addrs.iter().copied()).collect();

    let mut clusters = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let security = Security::new(
            ca.issue_node(id).expect("issue node cert"),
            ca.root_store().expect("trust root"),
        );
        let cluster = CraftCluster::builder(id, Kv::default())
            .members(ids)
            .allow_join(true)
            .raft_config(raft_config())
            .tick_period(Duration::from_millis(10))
            .start_quic(security, addrs[i], peers.clone())
            .await
            .expect("start quic node");
        clusters.push(Arc::new(cluster));
    }

    let leader = await_leader(&clusters).await;
    leader
        .handle()
        .propose(Cmd::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect("seed proposal");

    // Bring up node 4 knowing only node 1's address as a seed — no static entry
    // for nodes 2 and 3. `members` is the *current* voter set (without node 4),
    // so it starts as a non-voting follower and never disrupts the election.
    let joiner_id = NodeId(4);
    let joiner_addr = free_udp();
    let seed_only: PeerDirectory = [(NodeId(1), addrs[0])].into_iter().collect();
    let security = Security::new(
        ca.issue_node(joiner_id).expect("issue joiner cert"),
        ca.root_store().expect("trust root"),
    );
    let joiner = CraftCluster::builder(joiner_id, Kv::default())
        .members(ids)
        .allow_join(true)
        .raft_config(raft_config())
        .tick_period(Duration::from_millis(10))
        .join(NodeId(1), addrs[0])
        .start_quic(security, joiner_addr, seed_only)
        .await
        .expect("dynamic join over quic");
    let joiner = Arc::new(joiner);

    // The join committed a membership change adding node 4, and it caught up to
    // the pre-join state.
    let mut joined = false;
    for _ in 0..1000 {
        if let Some(status) = joiner.status().await
            && status.voters.contains(&joiner_id)
            && status.last_applied >= LogIndex(1)
        {
            joined = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(joined, "node 4 did not become a voter and catch up");

    // A fresh proposal now replicates to the enlarged cluster, node 4 included.
    let leader = await_leader(&clusters).await;
    leader
        .handle()
        .propose(Cmd::Set {
            key: "k2".into(),
            value: "v2".into(),
        })
        .await
        .expect("post-join proposal");
    let target = leader.status().await.expect("leader status").commit_index;

    let mut caught_up = false;
    for _ in 0..1000 {
        if let Some(status) = joiner.status().await
            && status.last_applied >= target
        {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(caught_up, "node 4 did not replicate the post-join proposal");

    joiner.shutdown();
    for c in &clusters {
        c.shutdown();
    }
}
