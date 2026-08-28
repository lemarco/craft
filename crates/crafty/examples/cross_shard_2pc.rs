//! Cross-shard two-phase commit on a local multi-Raft cluster (Tier 2).
//!
//! Mirrors the saga facade: [`CraftyCluster::run_keyed_2pc`] /
//! [`CraftyCluster::resume_cross_shard_2pc`] with a durable client journal on
//! Meta-Raft (and optional Redis when configured).
//!
//! Run with: `cargo run -p crafty --example cross_shard_2pc`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crafty::client::RemoteClient;
use crafty::core::{
    RaftGroupId, Role, StableShardRouter, StateMachine, TwoPhasePlan, TwoPhaseStep, place_shard,
};
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

#[derive(Default, Clone)]
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
        Ok(Resp::Ok)
    }

    fn query(&self, query: &Qry) -> Result<Resp, KvError> {
        match query {
            Qry::Get { key } => Ok(Resp::Value(self.map.get(key).cloned())),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        crafty::proto::encode(&self.map).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        self.map = crafty::proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

fn find_keys_for_two_groups(shard_count: u32, groups: &[RaftGroupId]) -> (Vec<u8>, Vec<u8>) {
    let router = StableShardRouter::new(shard_count);
    let mut key_a = None;
    let mut key_b = None;
    for i in 0u64..10_000 {
        let key = format!("route-{i}").into_bytes();
        let Some(shard) = router.shard_for(&key) else {
            continue;
        };
        match place_shard(shard, groups) {
            Some(RaftGroupId(0)) if key_a.is_none() => key_a = Some(key),
            Some(RaftGroupId(1)) if key_b.is_none() => key_b = Some(key),
            _ => {}
        }
        if key_a.is_some() && key_b.is_some() {
            break;
        }
    }
    (
        key_a.expect("key for group 0"),
        key_b.expect("key for group 1"),
    )
}

async fn wait_for_leaders(clusters: &[Arc<CraftyCluster<Kv>>], groups: u32) {
    for _ in 0..400 {
        let mut ready = true;
        for g in 0..groups {
            if !group_has_leader(clusters, g).await {
                ready = false;
                break;
            }
        }
        if ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for group leaders");
}

async fn group_has_leader(clusters: &[Arc<CraftyCluster<Kv>>], group: u32) -> bool {
    for c in clusters {
        if let Some(h) = c.group_handle(group)
            && h.status()
                .await
                .is_some_and(|s| matches!(s.role, Role::Leader))
        {
            return true;
        }
    }
    false
}

async fn await_leader(clusters: &[Arc<CraftyCluster<Kv>>]) -> Arc<CraftyCluster<Kv>> {
    for _ in 0..400 {
        for c in clusters {
            if c.is_leader().await {
                return Arc::clone(c);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for a cluster leader");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let cluster = CraftyCluster::builder(id, Kv::default())
            .members(ids)
            .tick_period(Duration::from_millis(10))
            .shard_count(64)
            .cross_shard_2pc(true)
            .durable_cross_shard_2pc(true)
            .raft_machines([Kv::default(), Kv::default()])
            .start_local(&net)
            .await;
        clusters.push(Arc::new(cluster));
    }

    wait_for_leaders(&clusters, 2).await;
    let leader = await_leader(&clusters).await;

    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (key_a, key_b) = find_keys_for_two_groups(64, &groups);
    let plan = TwoPhasePlan {
        tx_id: b"example-transfer".to_vec(),
        steps: vec![
            TwoPhaseStep {
                key: key_a,
                command: crafty::proto::encode(&Cmd::Set {
                    key: "from".into(),
                    value: "100".into(),
                })?,
            },
            TwoPhaseStep {
                key: key_b,
                command: crafty::proto::encode(&Cmd::Set {
                    key: "to".into(),
                    value: "200".into(),
                })?,
            },
        ],
    };

    let client = RemoteClient::new(Arc::new(net.clone()), [leader.node_id()]);
    leader.run_keyed_2pc(&client, &plan).await?;
    println!("cross-shard 2PC committed ✓");

    let journal = leader.two_phase_journal();
    let loaded = journal.load(&plan.tx_id).await?;
    assert!(loaded.is_some(), "client journal should record progress");
    println!(
        "journal prepared_steps = {}",
        loaded.unwrap().prepared_steps
    );

    for cluster in &clusters {
        cluster.shutdown();
    }
    Ok(())
}
