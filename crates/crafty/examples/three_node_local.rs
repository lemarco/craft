//! A three-node crafty cluster in a single process, driven by a **remote
//! client** that talks to any node and is transparently forwarded to the leader
//! (client-routing). This mirrors a real deployment (client → any node → leader)
//! without needing three machines or certificates — the nodes are wired over
//! the in-memory [`LocalNetwork`] the simulator and tests use.
//!
//! Run with: `cargo run -p crafty --example three_node_local`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crafty::client::{RetryPolicy, TypedClient};
use crafty::core::{Config, StateMachine};
use crafty::net::LocalNetwork;
use crafty::proto::LogIndex;
use crafty::{CraftyCluster, NodeId};
use serde::{Deserialize, Serialize};

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
        let Cmd::Set { key, value } = command;
        self.map.insert(key.clone(), value.clone());
        Ok(Resp::Set)
    }

    fn query(&self, query: &Qry) -> Result<Resp, KvError> {
        let Qry::Get { key } = query;
        Ok(Resp::Value(self.map.get(key).cloned()))
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        crafty::proto::encode(&self.map).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        self.map = crafty::proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();

    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(Config {
                election_timeout_min: 5,
                election_timeout_max: 10,
                heartbeat_interval: 2,
                seed: 7,
                ..Default::default()
            })
            .tick_period(Duration::from_millis(10))
            .start_local(&net)
            .await;
        clusters.push(cluster);
    }

    // Wait for a leader to emerge.
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
    println!("leader elected: {:?}", leader.expect("a leader"));

    // The client targets *all* nodes; it does not need to know the leader —
    // whichever node it hits forwards writes/reads to the leader for it.
    let remote =
        crafty::client::RemoteClient::new(Arc::new(net.clone()), ids).with_retry(RetryPolicy {
            max_attempts: 8,
            attempt_timeout: Duration::from_secs(2),
            backoff: Duration::from_millis(20),
        });
    let client: TypedClient<_, Kv> = TypedClient::new(remote);

    client
        .propose(&Cmd::Set {
            key: "region".into(),
            value: "eu-central".into(),
        })
        .await?;
    let got = client
        .query(&Qry::Get {
            key: "region".into(),
        })
        .await?;
    println!("get region -> {got:?}");
    assert_eq!(got, Resp::Value(Some("eu-central".into())));

    println!("three-node forwarding example OK ✓");
    for c in &clusters {
        c.shutdown();
    }
    Ok(())
}
