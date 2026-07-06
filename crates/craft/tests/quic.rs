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
