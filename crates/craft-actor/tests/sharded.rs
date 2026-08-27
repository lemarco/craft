//! Multi-Raft integration: shard-aware client routing through
//! [`ShardedNodeService`] (ADR 031).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_core::{RaftGroupId, Role, ShardRouter, StateMachine, place_shard};
use craft_actor::craft_net::{LocalNetwork, Transport, send_client_request};
use craft_actor::craft_proto::{ClientRequest, ClientResponse, LogIndex, NodeId};
use craft_actor::spawn_multi_raft_node;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum KvCommand {
    Set { key: String, value: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum KvQuery {
    Get { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KvResponse {
    Set { previous: Option<String> },
    Value(Option<String>),
}

#[derive(Debug, thiserror::Error)]
#[error("kv error")]
struct KvError;

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvMachine {
    map: BTreeMap<String, String>,
}

impl StateMachine for KvMachine {
    type Command = KvCommand;
    type Query = KvQuery;
    type Response = KvResponse;
    type Error = KvError;

    fn apply(
        &mut self,
        _index: LogIndex,
        command: &Self::Command,
    ) -> Result<Self::Response, Self::Error> {
        match command {
            KvCommand::Set { key, value } => {
                let previous = self.map.insert(key.clone(), value.clone());
                Ok(KvResponse::Set { previous })
            }
        }
    }

    fn query(&self, query: &Self::Query) -> Result<Self::Response, Self::Error> {
        match query {
            KvQuery::Get { key } => Ok(KvResponse::Value(self.map.get(key).cloned())),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        craft_actor::craft_proto::encode(self).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        *self = craft_actor::craft_proto::decode(snapshot).map_err(|_| KvError)?;
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
    let k0 = by_group
        .get(&groups[0].0)
        .expect("key for group 0")
        .clone();
    let k1 = by_group
        .get(&groups[1].0)
        .expect("key for group 1")
        .clone();
    (k0, k1)
}

async fn wait_for_group_leaders(handles: &[craft_actor::NodeHandle<KvMachine>]) {
    for _ in 0..500 {
        let mut leaders = 0usize;
        for handle in handles {
            if let Some(status) = handle.status().await {
                if status.role == Role::Leader {
                    leaders += 1;
                }
            }
        }
        if leaders == handles.len() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("not all raft groups elected a leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_writes_route_to_independent_raft_groups() {
    let net = LocalNetwork::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let node_id = NodeId(1);
    let members = [node_id];
    let shard_count = 64;
    let group_ids = [RaftGroupId(0), RaftGroupId(1)];
    let (route_key_a, route_key_b) = find_keys_for_two_groups(shard_count, &group_ids);

    let machines = vec![KvMachine::default(), KvMachine::default()];
    let (sharded, handles) = spawn_multi_raft_node(
        node_id,
        &members,
        craft_actor::craft_core::Config::default(),
        craft_actor::RuntimeConfig {
            tick_period: Duration::from_millis(10),
            allow_join: false,
        },
        shard_count,
        2,
        machines,
        Arc::clone(&transport),
        Duration::from_secs(5),
    );
    net.attach(node_id, Arc::new(sharded));

    wait_for_group_leaders(&handles).await;

    let cmd_a = craft_actor::craft_proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "group0".into(),
    })
    .unwrap();
    let cmd_b = craft_actor::craft_proto::encode(&KvCommand::Set {
        key: "a".into(),
        value: "group1".into(),
    })
    .unwrap();

    let resp_a = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_key_a.clone(),
            command: cmd_a,
        },
    )
    .await
    .expect("propose to group 0");
    let ClientResponse::Ok(bytes) = resp_a else {
        panic!("unexpected propose response: {resp_a:?}");
    };
    let set_a: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(set_a, KvResponse::Set { previous: None });

    let resp_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::ProposeKeyed {
            key: route_key_b.clone(),
            command: cmd_b,
        },
    )
    .await
    .expect("propose to group 1");
    let ClientResponse::Ok(bytes) = resp_b else {
        panic!("unexpected propose response: {resp_b:?}");
    };

    let qry_a = craft_actor::craft_proto::encode(&KvQuery::Get { key: "a".into() }).unwrap();
    let got_a = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_key_a,
            query: qry_a,
        },
    )
    .await
    .expect("query group 0");
    let ClientResponse::Ok(bytes) = got_a else {
        panic!("unexpected query response: {got_a:?}");
    };
    let val_a: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(val_a, KvResponse::Value(Some("group0".into())));

    let qry_b = craft_actor::craft_proto::encode(&KvQuery::Get { key: "a".into() }).unwrap();
    let got_b = send_client_request(
        &*transport,
        node_id,
        &ClientRequest::QueryKeyed {
            key: route_key_b,
            query: qry_b,
        },
    )
    .await
    .expect("query group 1");
    let ClientResponse::Ok(bytes) = got_b else {
        panic!("unexpected query response: {got_b:?}");
    };
    let val_b: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(val_b, KvResponse::Value(Some("group1".into())));

    for handle in handles {
        handle.shutdown();
    }
}
