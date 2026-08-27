//! Multi-Raft via [`CraftClusterBuilder::raft_machines`] (ADR 031).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use craft::CraftCluster;
use craft::core::{Config, RaftGroupId, Role, ShardRouter, StateMachine, place_shard};
use craft::net::{LocalNetwork, send_client_request};
use craft::proto::{ClientRequest, ClientResponse, LogIndex, NodeId};
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
        craft::proto::encode(self).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        *self = craft::proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

fn find_keys_for_two_groups(shard_count: u32, groups: &[RaftGroupId]) -> (Vec<u8>, Vec<u8>) {
    let router = ShardRouter::new(shard_count);
    let mut by_group: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    for i in 0..10_000u32 {
        let key = format!("route-{i}").into_bytes();
        let shard = router.shard_for(&key);
        let Some(group) = place_shard(shard, groups) else {
            continue;
        };
        by_group.entry(group.0).or_insert(key);
        if by_group.len() >= 2 {
            break;
        }
    }
    (
        by_group.get(&groups[0].0).expect("key0").clone(),
        by_group.get(&groups[1].0).expect("key1").clone(),
    )
}

async fn wait_for_group_leaders(cluster: &CraftCluster<KvMachine>) {
    for _ in 0..500 {
        let mut leaders = 0usize;
        for handle in cluster.group_handles() {
            if let Some(status) = handle.status().await
                && status.role == Role::Leader
            {
                leaders += 1;
            }
        }
        if leaders == cluster.raft_groups() as usize {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("not all raft groups elected a leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_hosts_independent_raft_groups() {
    let net = LocalNetwork::new();
    let node_id = NodeId(1);
    let shard_count = 64;
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let (route_a, route_b) = find_keys_for_two_groups(shard_count, &groups);

    let cluster = CraftCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(Config {
            election_timeout_min: 5,
            election_timeout_max: 10,
            heartbeat_interval: 2,
            seed: 3,
        })
        .tick_period(Duration::from_millis(10))
        .shard_count(shard_count)
        .raft_machines([KvMachine::default(), KvMachine::default()])
        .start_local(&net)
        .await;

    assert_eq!(cluster.raft_groups(), 2);
    assert_eq!(cluster.group_handles().len(), 2);

    wait_for_group_leaders(&cluster).await;

    let transport: Arc<dyn craft::net::Transport> = Arc::new(net.clone());
    let cmd_a = craft::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "g0".into(),
    })
    .unwrap();
    let cmd_b = craft::proto::encode(&KvCommand::Set {
        key: "k".into(),
        value: "g1".into(),
    })
    .unwrap();

    let resp = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_a.clone(),
            command: cmd_a,
        },
    )
    .await
    .expect("propose group 0");
    assert!(matches!(resp, ClientResponse::Ok(_)));

    let resp = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_b.clone(),
            command: cmd_b,
        },
    )
    .await
    .expect("propose group 1");
    assert!(matches!(resp, ClientResponse::Ok(_)));

    let qry = craft::proto::encode(&KvQuery::Get { key: "k".into() }).unwrap();
    let got_a = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_a,
            query: qry.clone(),
        },
    )
    .await
    .expect("query group 0");
    let ClientResponse::Ok(bytes) = got_a else {
        panic!("unexpected response: {got_a:?}");
    };
    let val: KvResponse = craft::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(Some("g0".into())));

    let got_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_b,
            query: qry,
        },
    )
    .await
    .expect("query group 1");
    let ClientResponse::Ok(bytes) = got_b else {
        panic!("unexpected response: {got_b:?}");
    };
    let val: KvResponse = craft::proto::decode(&bytes).unwrap();
    assert_eq!(val, KvResponse::Value(Some("g1".into())));

    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_persists_each_raft_group_to_separate_redb_files() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let node_id = NodeId(1);

    {
        let cluster = CraftCluster::builder(node_id, KvMachine::default())
            .members([node_id])
            .tick_period(Duration::from_millis(10))
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_group_leaders(&cluster).await;

        let resp = cluster.group_handles()[0]
            .propose(KvCommand::Set {
                key: "persist".into(),
                value: "g0".into(),
            })
            .await
            .expect("propose on group 0");
        assert_eq!(resp, KvResponse::Set);

        cluster.shutdown();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(data_dir.join("group-0.redb").is_file());
    assert!(data_dir.join("group-1.redb").is_file());

    let layout = craft::storage::GroupRedbLayout::new(&data_dir);
    let store = layout.open_group(0).unwrap();
    use craft::storage::LogStore;
    assert!(store.last_index().unwrap().0 >= 1);
}
