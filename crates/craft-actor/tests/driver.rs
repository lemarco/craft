//! Integration tests for [`craft_actor::RaftDriver`] — the runtime glue that
//! composes the Raft core with a user state machine.
//!
//! A reference in-memory key/value [`StateMachine`] is driven both as a
//! single-node cluster (synchronous commit/read paths) and as a routed
//! three-node cluster (real election, replication, and cross-node apply),
//! using only the driver's public `tick`/`deliver_*`/`propose`/`query` API and
//! the [`NetEffect`]s it surfaces.

use std::collections::{BTreeMap, HashMap};

use craft_actor::craft_core::StateMachine;
use craft_actor::craft_core::{Config, RaftNode, ReadId, Role};
use craft_actor::craft_proto::NodeId;
use craft_actor::{NetEffect, RaftDriver, ReadOutcome, Step};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Reference KV state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
enum KvCommand {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum KvQuery {
    Get { key: String },
    Len,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KvResponse {
    Set { previous: Option<String> },
    Deleted { existed: bool },
    Value(Option<String>),
    Len(u64),
}

#[derive(Debug, thiserror::Error)]
#[error("kv error: {0}")]
struct KvError(String);

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvMachine {
    map: BTreeMap<String, String>,
    /// Highest applied index, to assert exactly-once ordered application.
    applied_through: u64,
}

impl StateMachine for KvMachine {
    type Command = KvCommand;
    type Query = KvQuery;
    type Response = KvResponse;
    type Error = KvError;

    fn apply(
        &mut self,
        index: craft_actor::craft_proto::LogIndex,
        command: &Self::Command,
    ) -> Result<Self::Response, Self::Error> {
        // Commands apply in strictly ascending index order, exactly once. The
        // indices are not contiguous: the core interleaves non-command entries
        // (the leader's election no-op, membership entries) that never reach
        // the state machine.
        assert!(
            index.0 > self.applied_through,
            "commands must apply in strictly ascending index order exactly once \
             (index {} <= applied_through {})",
            index.0,
            self.applied_through
        );
        self.applied_through = index.0;
        Ok(match command {
            KvCommand::Set { key, value } => {
                let previous = self.map.insert(key.clone(), value.clone());
                KvResponse::Set { previous }
            }
            KvCommand::Delete { key } => {
                let existed = self.map.remove(key).is_some();
                KvResponse::Deleted { existed }
            }
        })
    }

    fn query(&self, query: &Self::Query) -> Result<Self::Response, Self::Error> {
        Ok(match query {
            KvQuery::Get { key } => KvResponse::Value(self.map.get(key).cloned()),
            KvQuery::Len => KvResponse::Len(self.map.len() as u64),
        })
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        craft_actor::craft_proto::encode(self).map_err(|e| KvError(e.to_string()))
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        *self = craft_actor::craft_proto::decode(snapshot).map_err(|e| KvError(e.to_string()))?;
        Ok(())
    }
}

fn config() -> Config {
    Config {
        election_timeout_min: 10,
        election_timeout_max: 20,
        heartbeat_interval: 3,
        seed: 42,
    }
}

fn single_node() -> RaftDriver<KvMachine> {
    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    RaftDriver::new(node, KvMachine::default())
}

// ---------------------------------------------------------------------------
// Single-node cluster: synchronous commit + read paths
// ---------------------------------------------------------------------------

#[test]
fn single_node_becomes_leader_on_campaign() {
    let mut d = single_node();
    let step = d.campaign().unwrap();
    assert!(d.is_leader());
    assert!(
        step.role_changes.contains(&Role::Leader),
        "campaign should surface a Leader role change"
    );
}

#[test]
fn single_node_propose_commits_and_applies_synchronously() {
    let mut d = single_node();
    d.campaign().unwrap();

    let (index, step) = d
        .propose(&KvCommand::Set {
            key: "a".into(),
            value: "1".into(),
        })
        .unwrap();

    // Index 1 is the leader's election no-op; the first client command is 2.
    assert_eq!(index.0, 2);
    assert_eq!(step.applied.len(), 1, "single-node commits immediately");
    let (applied_index, response) = &step.applied[0];
    assert_eq!(*applied_index, index);
    assert_eq!(*response, KvResponse::Set { previous: None });
}

#[test]
fn propose_on_follower_is_rejected_as_not_leader() {
    let mut d = single_node();
    // Never campaigned → still a follower.
    let err = d
        .propose(&KvCommand::Set {
            key: "a".into(),
            value: "1".into(),
        })
        .unwrap_err();
    assert!(matches!(err, craft_actor::DriverError::NotLeader { .. }));
}

#[test]
fn set_then_overwrite_reports_previous_value() {
    let mut d = single_node();
    d.campaign().unwrap();
    d.propose(&KvCommand::Set {
        key: "k".into(),
        value: "v1".into(),
    })
    .unwrap();
    let (_, step) = d
        .propose(&KvCommand::Set {
            key: "k".into(),
            value: "v2".into(),
        })
        .unwrap();
    assert_eq!(
        step.applied[0].1,
        KvResponse::Set {
            previous: Some("v1".into())
        }
    );
}

#[test]
fn single_node_linearizable_read_is_served_after_write() {
    let mut d = single_node();
    d.campaign().unwrap();
    d.propose(&KvCommand::Set {
        key: "a".into(),
        value: "42".into(),
    })
    .unwrap();

    let step = d
        .query(ReadId(7), KvQuery::Get { key: "a".into() })
        .unwrap();
    assert_eq!(step.reads.len(), 1, "single-node read confirms immediately");
    match &step.reads[0] {
        ReadOutcome::Ready { id, response } => {
            assert_eq!(*id, ReadId(7));
            assert_eq!(*response, KvResponse::Value(Some("42".into())));
        }
        ReadOutcome::Failed { .. } => panic!("read should succeed on a live leader"),
    }
}

#[test]
fn query_on_follower_is_rejected_as_not_leader() {
    let mut d = single_node();
    let err = d.query(ReadId(1), KvQuery::Len).unwrap_err();
    assert!(matches!(err, craft_actor::DriverError::NotLeader { .. }));
}

#[test]
fn delete_reports_existence() {
    let mut d = single_node();
    d.campaign().unwrap();
    let (_, step) = d
        .propose(&KvCommand::Delete {
            key: "ghost".into(),
        })
        .unwrap();
    assert_eq!(step.applied[0].1, KvResponse::Deleted { existed: false });

    d.propose(&KvCommand::Set {
        key: "real".into(),
        value: "x".into(),
    })
    .unwrap();
    let (_, step) = d
        .propose(&KvCommand::Delete { key: "real".into() })
        .unwrap();
    assert_eq!(step.applied[0].1, KvResponse::Deleted { existed: true });
}

// ---------------------------------------------------------------------------
// Three-node cluster: routed election, replication, and apply
// ---------------------------------------------------------------------------

/// A tiny synchronous message router over a set of drivers. It pumps the
/// [`NetEffect`]s each driver emits back into their destination drivers until
/// the network quiesces, exactly like a real transport would (minus the I/O).
struct Cluster {
    drivers: HashMap<NodeId, RaftDriver<KvMachine>>,
    ids: Vec<NodeId>,
    /// Reads resolved anywhere in the cluster during routing.
    reads: Vec<ReadOutcome<KvResponse>>,
}

/// A network effect tagged with the node that produced it (its sender).
struct Pending {
    from: NodeId,
    effect: NetEffect,
}

/// An application recorded at a node: (node, index, response).
type AppliedRecord = (NodeId, u64, KvResponse);

impl Cluster {
    fn new(ids: &[NodeId]) -> Self {
        let members: Vec<NodeId> = ids.to_vec();
        let drivers = members
            .iter()
            .map(|&id| {
                let node = RaftNode::new(id, members.clone(), config());
                (id, RaftDriver::new(node, KvMachine::default()))
            })
            .collect();
        Self {
            drivers,
            ids: members,
            reads: Vec::new(),
        }
    }

    /// Queue every effect in `step`, tagging each with its author `from`.
    fn enqueue(queue: &mut Vec<Pending>, from: NodeId, step: &Step<KvMachine>) {
        for effect in &step.effects {
            queue.push(Pending {
                from,
                effect: effect.clone(),
            });
        }
    }

    /// Deliver every queued effect (and any it triggers) until quiescent,
    /// collecting all applied responses produced anywhere in the cluster.
    fn pump(&mut self, mut queue: Vec<Pending>) -> Vec<AppliedRecord> {
        let mut applied = Vec::new();
        let mut guard = 0;
        while let Some(Pending { from, effect }) = queue.pop() {
            guard += 1;
            assert!(guard < 100_000, "router failed to quiesce");

            // The destination processes the effect; the resulting step is
            // authored by that destination node.
            let (author, step) = match effect {
                NetEffect::Send { peer, rpc } => {
                    let Some(target) = self.drivers.get_mut(&peer) else {
                        continue;
                    };
                    (peer, target.deliver_rpc(from, rpc).unwrap())
                }
                NetEffect::Reply { peer, reply } => {
                    let Some(target) = self.drivers.get_mut(&peer) else {
                        continue;
                    };
                    (peer, target.deliver_reply(from, reply).unwrap())
                }
            };

            for (idx, resp) in &step.applied {
                applied.push((author, idx.0, resp.clone()));
            }
            self.reads.extend(step.reads.iter().cloned());
            Self::enqueue(&mut queue, author, &step);
        }
        applied
    }

    /// Tick every node once, routing all resulting effects to quiescence.
    fn tick_all(&mut self) -> Vec<AppliedRecord> {
        let mut queue = Vec::new();
        for id in self.ids.clone() {
            let step = self.drivers.get_mut(&id).unwrap().tick().unwrap();
            Self::enqueue(&mut queue, id, &step);
        }
        self.pump(queue)
    }

    /// Run the cluster until a leader is elected (or panic after `max` rounds).
    fn elect_leader(&mut self, max: usize) -> NodeId {
        for _ in 0..max {
            self.tick_all();
            if let Some(leader) = self.leader() {
                return leader;
            }
        }
        panic!("no leader elected within {max} rounds");
    }

    /// Propose a command on `leader` and pump the cluster to quiescence,
    /// returning all applications recorded across the cluster.
    fn propose_on(&mut self, leader: NodeId, command: KvCommand) -> Vec<AppliedRecord> {
        let (_, step) = self
            .drivers
            .get_mut(&leader)
            .unwrap()
            .propose(&command)
            .unwrap();
        let mut applied: Vec<AppliedRecord> = step
            .applied
            .iter()
            .map(|(idx, resp)| (leader, idx.0, resp.clone()))
            .collect();
        let mut queue = Vec::new();
        Self::enqueue(&mut queue, leader, &step);
        applied.extend(self.pump(queue));
        applied
    }

    fn leader(&self) -> Option<NodeId> {
        self.ids
            .iter()
            .copied()
            .find(|id| self.drivers[id].is_leader())
    }

    /// How many nodes have applied at least `index` (i.e. `applied_through`).
    fn applied_count(&self, index: u64) -> usize {
        self.ids
            .iter()
            .filter(|id| self.drivers[id].machine().applied_through >= index)
            .count()
    }
}

#[test]
fn three_nodes_elect_a_single_leader() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = Cluster::new(&ids);
    cluster.elect_leader(200);

    let leaders: Vec<NodeId> = ids
        .iter()
        .copied()
        .filter(|id| cluster.drivers[id].is_leader())
        .collect();
    assert_eq!(leaders.len(), 1, "exactly one leader, got {leaders:?}");
}

#[test]
fn three_nodes_replicate_and_apply_a_command_everywhere() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = Cluster::new(&ids);
    let leader = cluster.elect_leader(200);

    let applied = cluster.propose_on(
        leader,
        KvCommand::Set {
            key: "shared".into(),
            value: "value".into(),
        },
    );

    // The leader must have applied the command with the expected response.
    assert!(
        applied
            .iter()
            .any(|(node, _, resp)| *node == leader && *resp == KvResponse::Set { previous: None }),
        "leader should apply the Set command, got {applied:?}"
    );

    // The command commits at index 2 (index 1 is the leader's no-op) and
    // eventually applies on a quorum. Give followers a few heartbeat rounds to
    // learn the advanced commit index and apply.
    for _ in 0..20 {
        cluster.tick_all();
    }
    assert!(
        cluster.applied_count(2) >= 2,
        "a quorum of nodes should apply the command at index 2"
    );
}

#[test]
fn three_node_leader_serves_linearizable_read_after_replication() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = Cluster::new(&ids);
    let leader = cluster.elect_leader(200);

    cluster.propose_on(
        leader,
        KvCommand::Set {
            key: "k".into(),
            value: "v".into(),
        },
    );

    // Register the read on the leader. ReadIndex needs a heartbeat quorum, so
    // it resolves only once a follower's ack flows back — pump delivers those
    // heartbeats and replies, and the cluster records the resolved read.
    let step = cluster
        .drivers
        .get_mut(&leader)
        .unwrap()
        .query(ReadId(1), KvQuery::Get { key: "k".into() })
        .unwrap();
    cluster.reads.extend(step.reads.iter().cloned());
    let mut queue = Vec::new();
    Cluster::enqueue(&mut queue, leader, &step);
    cluster.pump(queue);

    // A few heartbeat rounds guarantee the quorum ack arrives if it hasn't yet.
    for _ in 0..10 {
        cluster.tick_all();
    }

    let ready: Vec<&ReadOutcome<KvResponse>> = cluster
        .reads
        .iter()
        .filter(|r| matches!(r, ReadOutcome::Ready { id, .. } if *id == ReadId(1)))
        .collect();
    assert_eq!(
        ready.len(),
        1,
        "read should resolve exactly once: {:?}",
        cluster.reads
    );
    match ready[0] {
        ReadOutcome::Ready { response, .. } => {
            assert_eq!(*response, KvResponse::Value(Some("v".into())));
        }
        ReadOutcome::Failed { .. } => unreachable!(),
    }
}
