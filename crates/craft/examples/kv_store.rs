//! A minimal replicated key-value store on a **single-node** craft cluster.
//!
//! The smallest possible end-to-end program: define a [`StateMachine`], start a
//! one-node cluster, then propose writes and run linearizable reads through the
//! in-process handle. Scale it up by giving the builder more `members` and
//! starting each on its own process with [`CraftCluster::builder`]`.start_quic`.
//!
//! Run with: `cargo run -p craft --example kv_store`

use std::collections::BTreeMap;
use std::time::Duration;

use craft::core::StateMachine;
use craft::net::LocalNetwork;
use craft::proto::LogIndex;
use craft::{CraftCluster, NodeId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Cmd {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Serialize, Deserialize)]
enum Qry {
    Get { key: String },
}

#[derive(Debug, Serialize, Deserialize)]
enum Resp {
    Ok,
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
                Ok(Resp::Ok)
            }
            Cmd::Delete { key } => {
                self.map.remove(key);
                Ok(Resp::Ok)
            }
        }
    }

    fn query(&self, query: &Qry) -> Result<Resp, KvError> {
        match query {
            Qry::Get { key } => Ok(Resp::Value(self.map.get(key).cloned())),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        craft::proto::encode(&self.map).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        self.map = craft::proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let net = LocalNetwork::new();
    let cluster = CraftCluster::builder(NodeId(1), Kv::default())
        .tick_period(Duration::from_millis(10))
        .start_local(&net)
        .await;

    // A single node elects itself immediately.
    for _ in 0..200 {
        if cluster.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!("leader elected: {}", cluster.is_leader().await);

    let handle = cluster.handle();
    handle
        .propose(Cmd::Set {
            key: "greeting".into(),
            value: "hello, craft".into(),
        })
        .await?;
    handle
        .propose(Cmd::Set {
            key: "lang".into(),
            value: "rust".into(),
        })
        .await?;

    let got = handle
        .query(Qry::Get {
            key: "greeting".into(),
        })
        .await?;
    println!("get greeting -> {got:?}");
    assert!(matches!(got, Resp::Value(Some(ref v)) if v == "hello, craft"));

    handle
        .propose(Cmd::Delete {
            key: "greeting".into(),
        })
        .await?;
    let got = handle
        .query(Qry::Get {
            key: "greeting".into(),
        })
        .await?;
    println!("get greeting after delete -> {got:?}");
    assert!(matches!(got, Resp::Value(None)));

    println!("kv store example OK ✓");
    cluster.shutdown();
    Ok(())
}
