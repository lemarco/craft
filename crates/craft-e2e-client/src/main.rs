//! Concurrent QUIC client for Jepsen-lite E2E (`e2e/linearizability.sh`).
//!
//! Talks to a running `craft-node` cluster (Demo state machine: each propose
//! increments a counter; query returns the current count) and checks the
//! recorded history with [`craft_sim::History`] + [`craft_sim::Model`].

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use craft_client::{Client, RemoteClient, RetryPolicy};
use craft_net::load_pem_material;
use craft_net::{CertPaths, PeerDirectory, QuicTransport, client_config, client_endpoint};
use craft_proto::NodeId;
use craft_sim::{History, Model};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
enum Op {
    Inc,
    Read,
}

struct Counter;

impl Model for Counter {
    type State = u64;
    type Input = Op;
    type Output = u64;

    fn init(&self) -> u64 {
        0
    }

    fn apply(&self, state: &u64, input: &Op) -> (u64, u64) {
        match input {
            Op::Inc => {
                let next = state.saturating_add(1);
                (next, next)
            }
            Op::Read => (*state, *state),
        }
    }
}

fn env(key: &str) -> Result<String, String> {
    env::var(key).map_err(|_| format!("missing env {key}"))
}

fn parse_peers(raw: &str) -> Result<PeerDirectory, String> {
    let mut map = PeerDirectory::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad peer entry {entry:?} (want id@host:port)"))?;
        let id = id
            .parse::<u64>()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e| format!("bad addr in {entry:?}: {e}"))?;
        map.insert(NodeId(id), addr);
    }
    if map.is_empty() {
        return Err("CRAFT_PEERS is empty".into());
    }
    Ok(map)
}

fn decode_u64(bytes: &[u8]) -> Result<u64, String> {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    Ok(u64::from_le_bytes(buf))
}

async fn wait_cluster_ready(client: &RemoteClient) -> Result<(), String> {
    for attempt in 0..90 {
        if client.query(vec![]).await.is_ok() {
            return Ok(());
        }
        if attempt == 89 {
            return Err("cluster did not become queryable within 90s".into());
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId(
        env("CRAFT_NODE_ID")?
            .parse()
            .map_err(|_| "CRAFT_NODE_ID must be u64")?,
    );
    let peers = parse_peers(&env("CRAFT_PEERS")?)?;
    let paths = CertPaths::new(
        env("CRAFT_NODE_CERT")?,
        env("CRAFT_NODE_KEY")?,
        env("CRAFT_CA_CERT")?,
    );
    let rounds: u64 = env::var("CRAFT_LIN_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);

    let material = load_pem_material(node_id, &paths)?;
    let client_cfg = client_config(&material.identity, material.roots)?;
    let endpoint = client_endpoint("0.0.0.0:0".parse()?)?;
    let transport = Arc::new(QuicTransport::new(endpoint, client_cfg, peers));
    let client = Arc::new(
        RemoteClient::new(transport, [NodeId(1), NodeId(2), NodeId(3)]).with_retry(RetryPolicy {
            max_attempts: 8,
            attempt_timeout: std::time::Duration::from_secs(10),
            backoff: std::time::Duration::from_millis(250),
        }),
    );

    wait_cluster_ready(client.as_ref()).await?;

    let history = Arc::new(Mutex::new(History::<Op, u64>::new()));

    for round in 0..rounds {
        let client = Arc::clone(&client);
        let history = Arc::clone(&history);

        {
            let mut h = history.lock().await;
            h.invoke(0, Op::Inc);
            h.invoke(1, Op::Read);
        }

        let client_inc = Arc::clone(&client);
        let client_read = Arc::clone(&client);
        let (inc, read) = tokio::join!(
            async move { client_inc.propose(vec![]).await },
            async move { client_read.query(vec![]).await }
        );

        let inc_val = decode_u64(&inc.map_err(|e| format!("propose round {round}: {e}"))?)?;
        let read_val = decode_u64(&read.map_err(|e| format!("query round {round}: {e}"))?)?;

        let mut h = history.lock().await;
        h.response(0, inc_val);
        h.response(1, read_val);
    }

    let h = history.lock().await;
    if !h.is_linearizable(&Counter) {
        eprintln!("FAIL: QUIC client history is not linearizable");
        std::process::exit(1);
    }

    println!("LINEARIZABLE OK ({rounds} concurrent inc/read rounds over QUIC)");
    Ok(())
}
