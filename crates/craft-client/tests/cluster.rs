//! End-to-end client tests against a real 3-node cluster wired over the
//! in-memory `LocalNetwork` transport: the [`RemoteClient`]/[`TypedClient`]
//! drive live nodes through `craft_actor`'s `NodeService`, exercising
//! transparent follower→leader forwarding (ADR 003) and failover/retry (F4).

use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_core::{Config, RaftNode, Role, StateMachine};
use craft_actor::craft_proto::{LogIndex, NodeId};
use craft_actor::{NodeHandle, NodeService, RaftDriver, RuntimeConfig, spawn_node};
use craft_client::{Client, RemoteClient, RetryPolicy, TypedClient};
use craft_net::LocalNetwork;
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

#[derive(Debug, thiserror::Error)]
#[error("kv error")]
struct KvError;

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
        craft_actor::craft_proto::encode(&self.map).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        self.map = craft_actor::craft_proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

fn config() -> Config {
    Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed: 7,
    }
}

/// Spawn a 3-node cluster on a fresh `LocalNetwork` and return the network plus
/// each node's handle.
fn spawn_cluster() -> (LocalNetwork, Vec<(NodeId, NodeHandle<Kv>)>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let transport: Arc<dyn craft_net::Transport> = Arc::new(net.clone());

    let mut handles = Vec::new();
    for &id in &ids {
        let node = RaftNode::new(id, ids, config());
        let driver = RaftDriver::new(node, Kv::default());
        let cfg = RuntimeConfig {
            tick_period: Duration::from_millis(10),
            allow_join: false,
        };
        let handle = spawn_node(driver, Arc::clone(&transport), cfg);
        let service = NodeService::new(handle.clone(), Arc::clone(&transport));
        net.attach(id, Arc::new(service));
        handles.push((id, handle));
    }
    (net, handles)
}

/// Poll node statuses until one reports `Leader`, or panic after `~5s`.
async fn await_leader(handles: &[(NodeId, NodeHandle<Kv>)]) -> NodeId {
    for _ in 0..500 {
        for (id, handle) in handles {
            if let Some(status) = handle.status().await {
                if status.role == Role::Leader {
                    return *id;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected");
}

#[tokio::test]
async fn typed_client_proposes_and_reads_through_any_node() {
    let (net, handles) = spawn_cluster();
    let leader = await_leader(&handles).await;

    // Client contacts all three nodes; forwarding means it need not know which
    // one is the leader.
    let remote = RemoteClient::new(Arc::new(net.clone()), [NodeId(1), NodeId(2), NodeId(3)]);
    let client: TypedClient<RemoteClient, Kv> = TypedClient::new(remote);

    let resp = client
        .propose(&Cmd::Set {
            key: "a".into(),
            value: "1".into(),
        })
        .await
        .expect("propose");
    assert_eq!(resp, Resp::Set);

    let resp = client
        .query(&Qry::Get { key: "a".into() })
        .await
        .expect("query");
    assert_eq!(resp, Resp::Value(Some("1".into())));

    // Sanity: the leader we elected is one of the configured nodes.
    assert!([NodeId(1), NodeId(2), NodeId(3)].contains(&leader));

    for (_, h) in &handles {
        h.shutdown();
    }
}

#[tokio::test]
async fn client_targeting_only_a_follower_is_forwarded_to_the_leader() {
    let (net, handles) = spawn_cluster();
    let leader = await_leader(&handles).await;

    // Pick a follower as the *only* target: the write must still succeed via
    // transparent server-side forwarding (ADR 003).
    let follower = handles
        .iter()
        .map(|(id, _)| *id)
        .find(|id| *id != leader)
        .unwrap();

    let remote = RemoteClient::new(Arc::new(net.clone()), [follower]);
    let client: TypedClient<RemoteClient, Kv> = TypedClient::new(remote);

    let resp = client
        .propose(&Cmd::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect("follower forwards write to leader");
    assert_eq!(resp, Resp::Set);

    let resp = client
        .query(&Qry::Get { key: "k".into() })
        .await
        .expect("follower forwards read to leader");
    assert_eq!(resp, Resp::Value(Some("v".into())));

    for (_, h) in &handles {
        h.shutdown();
    }
}

#[tokio::test]
async fn client_fails_over_when_the_first_target_is_unreachable() {
    let (net, handles) = spawn_cluster();
    let _leader = await_leader(&handles).await;

    // Detach node 1 from the switch (crash/partition). A client whose first
    // rotation may land on the dead node must still succeed by retrying others.
    assert!(net.detach(NodeId(1)));

    let remote = RemoteClient::new(Arc::new(net.clone()), [NodeId(1), NodeId(2), NodeId(3)])
        .with_retry(RetryPolicy {
            max_attempts: 8,
            attempt_timeout: Duration::from_secs(2),
            backoff: Duration::from_millis(20),
        });

    // Use the raw byte API here to exercise `Client` directly.
    let payload = craft_actor::craft_proto::encode(&Cmd::Set {
        key: "x".into(),
        value: "y".into(),
    })
    .unwrap();
    let bytes = remote.propose(payload).await.expect("failover write");
    let resp: Resp = craft_actor::craft_proto::decode(&bytes).unwrap();
    assert_eq!(resp, Resp::Set);

    for (_, h) in &handles {
        h.shutdown();
    }
}
