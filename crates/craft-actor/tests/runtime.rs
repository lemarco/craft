//! End-to-end tests for [`craft_actor::spawn_node`] — the async node runtime.
//!
//! Three real runtime tasks are wired together over the in-memory
//! [`LocalNetwork`] transport (the same `Transport` port the live QUIC stack
//! implements). They elect a leader through timer-driven ticks and serve client
//! proposals/queries both directly via [`NodeHandle`] and over the
//! `/client/wire` path through the [`NodeService`] request handler.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_core::{Config, RaftNode, StateMachine};
use craft_actor::craft_net::{LocalNetwork, RequestHandler, Transport, send_client_request};
use craft_actor::craft_proto::{ClientRequest, ClientResponse, NodeId};
use craft_actor::{ClientError, NodeHandle, NodeService, RaftDriver, RuntimeConfig, spawn_node};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Reference KV state machine
// ---------------------------------------------------------------------------

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
        _index: craft_actor::craft_proto::LogIndex,
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

fn config() -> Config {
    Config {
        election_timeout_min: 8,
        election_timeout_max: 16,
        heartbeat_interval: 2,
        seed: 7,
    }
}

/// A running cluster of async node runtimes wired over one `LocalNetwork`.
struct Cluster {
    handles: HashMap<NodeId, NodeHandle<KvMachine>>,
    ids: Vec<NodeId>,
    net: LocalNetwork,
}

impl Cluster {
    fn start(ids: &[NodeId]) -> Self {
        let net = LocalNetwork::new();
        let mut handles = HashMap::new();
        for &id in ids {
            let node = RaftNode::new(id, ids.iter().copied(), config());
            let driver = RaftDriver::new(node, KvMachine::default());
            let transport: Arc<dyn Transport> = Arc::new(net.clone());
            let handle = spawn_node(
                driver,
                transport,
                RuntimeConfig {
                    tick_period: Duration::from_millis(5),
                },
            );
            let service: Arc<dyn RequestHandler> = Arc::new(NodeService::new(handle.clone()));
            net.attach(id, service);
            handles.insert(id, handle);
        }
        Self {
            handles,
            ids: ids.to_vec(),
            net,
        }
    }

    /// Poll node statuses until exactly one leader exists, or panic on timeout.
    async fn wait_for_leader(&self) -> NodeId {
        let deadline = Duration::from_secs(5);
        let poll = async {
            loop {
                let mut leaders = Vec::new();
                for &id in &self.ids {
                    if let Some(status) = self.handles[&id].status().await {
                        if matches!(status.role, craft_actor::craft_core::Role::Leader) {
                            leaders.push(id);
                        }
                    }
                }
                if leaders.len() == 1 {
                    return leaders[0];
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(deadline, poll)
            .await
            .expect("cluster failed to elect a single leader in time")
    }

    fn shutdown(&self) {
        for handle in self.handles.values() {
            handle.shutdown();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_a_leader() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;
    assert!(ids.contains(&leader));
    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_serves_propose_and_linearizable_query() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    let set = cluster.handles[&leader]
        .propose(KvCommand::Set {
            key: "color".into(),
            value: "amber".into(),
        })
        .await
        .expect("propose should succeed on the leader");
    assert_eq!(set, KvResponse::Set { previous: None });

    let got = cluster.handles[&leader]
        .query(KvQuery::Get {
            key: "color".into(),
        })
        .await
        .expect("query should succeed on the leader");
    assert_eq!(got, KvResponse::Value(Some("amber".into())));

    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_on_a_follower_reports_not_leader() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;
    let follower = ids.into_iter().find(|id| *id != leader).unwrap();

    let err = cluster.handles[&follower]
        .propose(KvCommand::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect_err("a follower must reject a direct proposal");
    match err {
        ClientError::NotLeader { leader: hint } => {
            // The follower usually knows who the leader is.
            if let Some(hint) = hint {
                assert_eq!(hint, leader);
            }
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_wire_propose_and_query_round_trip() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cluster = Cluster::start(&ids);
    let leader = cluster.wait_for_leader().await;

    // Talk to the leader the way a remote client would: encoded ClientRequests
    // over the `/client/wire` route via the transport.
    let propose = ClientRequest::Propose(
        craft_actor::craft_proto::encode(&KvCommand::Set {
            key: "lang".into(),
            value: "rust".into(),
        })
        .unwrap(),
    );
    let response = send_client_request(&cluster.net, leader, &propose)
        .await
        .expect("client wire propose");
    match response {
        ClientResponse::Ok(bytes) => {
            let decoded: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
            assert_eq!(decoded, KvResponse::Set { previous: None });
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    let query = ClientRequest::Query(
        craft_actor::craft_proto::encode(&KvQuery::Get { key: "lang".into() }).unwrap(),
    );
    let response = send_client_request(&cluster.net, leader, &query)
        .await
        .expect("client wire query");
    match response {
        ClientResponse::Ok(bytes) => {
            let decoded: KvResponse = craft_actor::craft_proto::decode(&bytes).unwrap();
            assert_eq!(decoded, KvResponse::Value(Some("rust".into())));
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    cluster.shutdown();
}
