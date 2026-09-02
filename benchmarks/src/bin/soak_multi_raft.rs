//! Multi-Raft in-process soak: keyed proposes + leader partition/heal (testing-strategy).
//!
//! Configure via env:
//!   SOAK_MULTI_SECS   wall-clock budget (default 15)
//!   SOAK_MULTI_SEED   RNG base (default 0xMULT1)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use trembita::cluster::TrembitaCluster;
use trembita::core::{Config, RaftGroupId, StableShardRouter, StateMachine, place_shard};
use trembita::net::{LocalNetwork, Transport, send_client_request};
use trembita::proto::{ClientRequest, ClientResponse, LogIndex, NodeId};
use trembita_benchmarks::TinyRng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvMachine {
    map: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvCommand {
    Set { key: String, value: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvQuery {
    Get { key: String },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KvResponse {
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

impl StateMachine for KvMachine {
    type Command = KvCommand;
    type Query = KvQuery;
    type Response = KvResponse;
    type Error = KvError;

    fn apply(&mut self, _index: LogIndex, command: &KvCommand) -> Result<KvResponse, KvError> {
        match command {
            KvCommand::Set { key, value } => {
                self.map.insert(key.clone(), value.clone());
                Ok(KvResponse::Set)
            }
        }
    }

    fn query(&self, query: &KvQuery) -> Result<KvResponse, KvError> {
        match query {
            KvQuery::Get { key } => Ok(KvResponse::Value(self.map.get(key).cloned())),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        trembita::proto::encode(self).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        *self = trembita::proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn raft_config(seed: u64) -> Config {
    Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed,
        ..Default::default()
    }
}

fn route_key(seed: u64, round: u64) -> Vec<u8> {
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let router = StableShardRouter::new(64);
    for i in 0..50_000u32 {
        let key = format!("soak-{seed}-{round}-{i}").into_bytes();
        let Some(shard) = router.shard_for(&key) else {
            continue;
        };
        if place_shard(shard, &groups).is_some() {
            return key;
        }
    }
    b"soak-fallback".to_vec()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let budget = Duration::from_secs(env_u64("SOAK_MULTI_SECS", 15));
    let base_seed = env_u64("SOAK_MULTI_SEED", 0x0B00_7001);

    println!(
        "soak_multi_raft: {budget:?} budget (seed base {base_seed:#x})"
    );

    let net = LocalNetwork::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = TrembitaCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(raft_config(base_seed ^ id.0))
            .tick_period(Duration::from_millis(10))
            .shard_count(64)
            .group_replication_factor(64)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let start = Instant::now();
    let mut rng = TinyRng::new(base_seed);
    let mut rounds = 0u64;
    let mut writes = 0u64;

    while start.elapsed() < budget {
        rounds += 1;
        let round_seed = base_seed ^ rounds;
        let key = route_key(base_seed, rounds);
        let cmd = trembita::proto::encode(&KvCommand::Set {
            key: String::from_utf8_lossy(&key).into_owned(),
            value: format!("v{rounds}"),
        })
        .expect("encode");

        let target = ids[(rng.next_u64() as usize) % ids.len()];
        if rng.next_u64().is_multiple_of(8) {
            let victim = ids[(rng.next_u64() as usize) % ids.len()];
            if net.detach(victim) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let cluster = clusters
                    .iter()
                    .find(|c| c.node_id() == victim)
                    .expect("cluster");
                net.attach(victim, cluster.wire_handler());
            }
        }

        let resp = send_client_request(
            &*transport,
            target,
            &ClientRequest::ProposeKeyed {
                key: key.clone(),
                command: cmd,
            },
        )
        .await;
        if matches!(resp, Ok(ClientResponse::Ok(_))) {
            writes += 1;
        }

        let _ = round_seed;
    }

    for c in clusters {
        c.shutdown();
    }

    println!(
        "soak_multi_raft done: rounds={rounds} keyed_writes_ok={writes} elapsed={:?}",
        start.elapsed()
    );
    assert!(writes > 0, "expected at least one successful keyed write");
}
