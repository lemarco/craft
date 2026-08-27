//! Integration tests for [`craft_actor::RaftDriver`] — the runtime glue that
//! composes the Raft core with a user state machine.
//!
//! A reference in-memory key/value [`StateMachine`] is driven both as a
//! single-node cluster (synchronous commit/read paths) and as a routed
//! three-node cluster (real election, replication, and cross-node apply),
//! using only the driver's public `tick`/`deliver_*`/`propose`/`query` API and
//! the [`NetEffect`]s it surfaces.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use craft_actor::craft_core::{Config, RaftNode, ReadId, Role, StateMachine};
use craft_actor::craft_proto::{EntryPayload, LogEntry, LogId, LogIndex, Membership, NodeId, Term};
use craft_actor::craft_storage::{
    HardState, HardStateStore, LogStore, MemoryStorage, Snapshot, SnapshotMeta, SnapshotStore,
    StorageError,
};
use craft_actor::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
use craft_test_support::{KvCommand, KvQuery, KvResponse, TrackedKv};

// ---------------------------------------------------------------------------
// Reference KV state machine (see `craft-test-support`)
// ---------------------------------------------------------------------------

fn config() -> Config {
    Config {
        election_timeout_min: 10,
        election_timeout_max: 20,
        heartbeat_interval: 3,
        seed: 42,
        ..Default::default()
    }
}

fn single_node() -> RaftDriver<TrackedKv> {
    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    RaftDriver::new(node, TrackedKv::default())
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
        ReadOutcome::Confirmed { .. } => panic!("leader read should not return Confirmed"),
    }
}

#[test]
fn leader_confirms_read_index_without_executing_query() {
    let mut d = single_node();
    d.campaign().unwrap();
    d.propose(&KvCommand::Set {
        key: "a".into(),
        value: "42".into(),
    })
    .unwrap();

    let step = d.confirm_read_index(ReadId(9)).unwrap();
    assert_eq!(step.reads.len(), 1);
    match &step.reads[0] {
        ReadOutcome::Confirmed { id, index } => {
            assert_eq!(*id, ReadId(9));
            assert!(index.0 >= 1);
        }
        other => panic!("expected Confirmed, got {other:?}"),
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
    drivers: HashMap<NodeId, RaftDriver<TrackedKv>>,
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
                (id, RaftDriver::new(node, TrackedKv::default()))
            })
            .collect();
        Self {
            drivers,
            ids: members,
            reads: Vec::new(),
        }
    }

    /// Queue every effect in `step`, tagging each with its author `from`.
    fn enqueue(queue: &mut Vec<Pending>, from: NodeId, step: &Step<TrackedKv>) {
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
            .filter(|id| self.drivers[id].machine().applied_through() >= index)
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
        ReadOutcome::Confirmed { .. } => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Durable persistence + restart recovery (B4)
// ---------------------------------------------------------------------------

/// A [`MemoryStorage`] shared behind an `Arc<Mutex<..>>` so a simulated restart
/// can drop one driver and hand the *same* durable bytes to a freshly recovered
/// one — exactly what a real on-disk backend does across a process restart.
#[derive(Clone, Default)]
struct SharedStorage(Arc<Mutex<MemoryStorage>>);

impl HardStateStore for SharedStorage {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        self.0.lock().unwrap().load_hard_state()
    }
    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.0.lock().unwrap().save_hard_state(state)
    }
}

impl LogStore for SharedStorage {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        self.0.lock().unwrap().first_index()
    }
    fn last_index(&self) -> Result<LogIndex, StorageError> {
        self.0.lock().unwrap().last_index()
    }
    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        self.0.lock().unwrap().read(index)
    }
    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        self.0.lock().unwrap().read_from(from)
    }
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        self.0.lock().unwrap().append(entries)
    }
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        self.0.lock().unwrap().truncate_suffix(from)
    }
    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        self.0.lock().unwrap().purge_prefix(through)
    }
}

impl SnapshotStore for SharedStorage {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.0.lock().unwrap().save_snapshot(snapshot)
    }
    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        self.0.lock().unwrap().load_snapshot()
    }
}

fn set(key: &str, value: &str) -> KvCommand {
    KvCommand::Set {
        key: key.into(),
        value: value.into(),
    }
}

#[test]
fn writes_are_persisted_to_storage_as_they_commit() {
    let storage = SharedStorage::default();
    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    let mut d = RaftDriver::with_storage(node, TrackedKv::default(), Box::new(storage.clone()));

    d.campaign().unwrap();
    d.propose(&set("a", "1")).unwrap();
    d.propose(&set("b", "2")).unwrap();

    // Log: index 1 = election no-op, 2 = Set a, 3 = Set b — all durable.
    assert_eq!(storage.last_index().unwrap(), LogIndex(3));
    assert_eq!(storage.first_index().unwrap(), LogIndex(1));
    let entries = storage.read_from(LogIndex(1)).unwrap();
    assert_eq!(entries.len(), 3, "no-op + two commands persisted");
    assert_eq!(entries[0].index, LogIndex(1));
    assert_eq!(entries[2].index, LogIndex(3));

    // The hard state reflects the elected term with a vote for self.
    let hard = storage.load_hard_state().unwrap();
    assert_eq!(hard.current_term, d.node().current_term());
    assert_eq!(hard.voted_for, Some(NodeId(1)));
}

#[test]
fn state_survives_a_restart_and_replays_committed_log() {
    let storage = SharedStorage::default();

    // ---- First life: elect, write three commands (a is overwritten). --------
    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    let mut d = RaftDriver::with_storage(node, TrackedKv::default(), Box::new(storage.clone()));
    d.campaign().unwrap();
    d.propose(&set("a", "1")).unwrap();
    d.propose(&set("b", "2")).unwrap();
    d.propose(&set("a", "3")).unwrap();

    let term_before = d.node().current_term();
    let last_before = d.node().last_log_index();
    assert_eq!(last_before, LogIndex(4), "no-op + three commands");

    // ---- Crash: drop the driver; only `storage` survives. -------------------
    drop(d);

    // ---- Recovery: rebuild from storage with a *fresh* state machine. -------
    let mut recovered = RaftDriver::recover(
        NodeId(1),
        [NodeId(1)],
        config(),
        TrackedKv::default(),
        Box::new(storage.clone()),
    )
    .unwrap();

    // Durable state came back verbatim; volatile state reset to a follower.
    assert_eq!(recovered.node().current_term(), term_before);
    assert_eq!(recovered.node().last_log_index(), last_before);
    assert!(
        !recovered.is_leader(),
        "a restarted node starts as a follower"
    );
    assert_eq!(
        recovered.machine().applied_through(),
        0,
        "the fresh machine has not replayed anything yet"
    );

    // Re-establishing leadership re-commits the recovered log and replays every
    // command into the fresh machine (no snapshot ⇒ full-log replay).
    recovered.campaign().unwrap();
    assert!(recovered.is_leader());

    let step = recovered
        .query(ReadId(1), KvQuery::Get { key: "a".into() })
        .unwrap();
    let ready = step
        .reads
        .iter()
        .find_map(|r| match r {
            ReadOutcome::Ready { response, .. } => Some(response.clone()),
            ReadOutcome::Failed { .. } => None,
            ReadOutcome::Confirmed { .. } => None,
        })
        .expect("read should resolve on the recovered single-node leader");
    assert_eq!(
        ready,
        KvResponse::Value(Some("3".into())),
        "last write to `a` must win after replay"
    );

    let step = recovered.query(ReadId(2), KvQuery::Len).unwrap();
    let len = step.reads.iter().find_map(|r| match r {
        ReadOutcome::Ready {
            response: KvResponse::Len(n),
            ..
        } => Some(*n),
        _ => None,
    });
    assert_eq!(len, Some(2), "keys `a` and `b` were replayed");
}

#[test]
fn snapshot_is_persisted_and_restored_across_a_restart() {
    let storage = SharedStorage::default();

    // ---- First life: elect, write, then compact through the applied index. --
    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    let mut d = RaftDriver::with_storage(node, TrackedKv::default(), Box::new(storage.clone()));
    d.campaign().unwrap();
    d.propose(&set("a", "1")).unwrap(); // index 2
    d.propose(&set("b", "2")).unwrap(); // index 3
    d.propose(&set("a", "3")).unwrap(); // index 4 (overwrites a)
    assert_eq!(d.node().last_applied(), LogIndex(4));

    // Compact: snapshots state through index 4 and purges indices 1..=4.
    assert!(d.compact().unwrap(), "there is applied state to compact");
    assert_eq!(d.node().snapshot_index(), LogIndex(4));
    assert_eq!(
        storage.first_index().unwrap(),
        LogIndex(5),
        "the compacted prefix is purged durably"
    );
    assert!(
        storage.load_snapshot().unwrap().is_some(),
        "the snapshot is stored durably"
    );

    // A further write lands beyond the boundary and is durable on its own.
    d.propose(&set("c", "4")).unwrap(); // index 5
    assert_eq!(storage.last_index().unwrap(), LogIndex(5));

    let term_before = d.node().current_term();

    // ---- Crash: only `storage` (snapshot + log suffix) survives. ------------
    drop(d);

    // ---- Recovery: restore the machine from the snapshot, resume the log. ---
    let mut recovered = RaftDriver::recover(
        NodeId(1),
        [NodeId(1)],
        config(),
        TrackedKv::default(),
        Box::new(storage.clone()),
    )
    .unwrap();

    assert_eq!(recovered.node().current_term(), term_before);
    assert_eq!(recovered.node().snapshot_index(), LogIndex(4));
    assert_eq!(recovered.node().last_applied(), LogIndex(4));
    assert_eq!(recovered.node().last_log_index(), LogIndex(5));
    assert_eq!(
        recovered.machine().applied_through(),
        4,
        "the snapshot restored applied state through its boundary"
    );
    assert!(
        !recovered.is_leader(),
        "a restarted node starts as a follower"
    );

    // Re-establishing leadership replays only the retained suffix (`set c`)
    // into the machine — the snapshotted prefix is never re-applied.
    recovered.campaign().unwrap();
    assert!(recovered.is_leader());

    let step = recovered.query(ReadId(1), KvQuery::Len).unwrap();
    let len = step.reads.iter().find_map(|r| match r {
        ReadOutcome::Ready {
            response: KvResponse::Len(n),
            ..
        } => Some(*n),
        _ => None,
    });
    assert_eq!(len, Some(3), "a, b and c are all present after recovery");

    let step = recovered
        .query(ReadId(2), KvQuery::Get { key: "a".into() })
        .unwrap();
    let a = step.reads.iter().find_map(|r| match r {
        ReadOutcome::Ready { response, .. } => Some(response.clone()),
        _ => None,
    });
    assert_eq!(
        a,
        Some(KvResponse::Value(Some("3".into()))),
        "the snapshotted last-write-wins value is preserved"
    );
}

#[test]
fn a_follower_persists_an_installed_snapshot() {
    use craft_actor::craft_proto::{InstallSnapshot, LogId, Membership, RaftRpc, Term};

    let storage = SharedStorage::default();
    let node = RaftNode::new(NodeId(2), [NodeId(1), NodeId(2), NodeId(3)], config());
    let mut d = RaftDriver::with_storage(node, TrackedKv::default(), Box::new(storage.clone()));

    // A leader's snapshot: application state {x: "1"} applied through index 4.
    let mut origin = TrackedKv::default();
    origin
        .apply(
            LogIndex(4),
            &KvCommand::Set {
                key: "x".into(),
                value: "1".into(),
            },
        )
        .unwrap();
    let data = craft_actor::craft_proto::encode(&origin).unwrap();

    let install = InstallSnapshot {
        term: Term(1),
        leader_id: NodeId(1),
        last_included: LogId::new(Term(1), LogIndex(4)),
        last_config: Membership {
            voters: vec![NodeId(1), NodeId(2), NodeId(3)],
            voters_outgoing: vec![],
            learners: vec![],
        },
        offset: 0,
        data,
        done: true,
    };
    d.deliver_rpc(NodeId(1), RaftRpc::InstallSnapshot(install))
        .unwrap();

    // The state machine was restored from the snapshot bytes.
    assert_eq!(d.node().snapshot_index(), LogIndex(4));
    assert_eq!(d.machine().applied_through(), 4);
    let got = d
        .machine()
        .query(&KvQuery::Get { key: "x".into() })
        .unwrap();
    assert_eq!(got, KvResponse::Value(Some("1".into())));

    // ...and the snapshot + purged prefix + advanced term are all durable.
    let stored = storage
        .load_snapshot()
        .unwrap()
        .expect("the installed snapshot is persisted");
    assert_eq!(stored.meta.last_included, LogId::new(Term(1), LogIndex(4)));
    assert_eq!(
        storage.first_index().unwrap(),
        LogIndex(5),
        "the compacted prefix is purged durably"
    );
    assert_eq!(storage.load_hard_state().unwrap().current_term, Term(1));
}

#[test]
fn recovered_node_persists_a_higher_term_after_restart() {
    let storage = SharedStorage::default();

    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    let mut d = RaftDriver::with_storage(node, TrackedKv::default(), Box::new(storage.clone()));
    d.campaign().unwrap();
    let term_before = d.node().current_term();
    drop(d);

    let mut recovered = RaftDriver::recover(
        NodeId(1),
        [NodeId(1)],
        config(),
        TrackedKv::default(),
        Box::new(storage.clone()),
    )
    .unwrap();
    recovered.campaign().unwrap();

    let term_after = recovered.node().current_term();
    assert!(
        term_after > term_before,
        "post-restart election must advance the term ({term_after:?} > {term_before:?})"
    );
    // The advanced term is durable, not just in memory.
    assert_eq!(storage.load_hard_state().unwrap().current_term, term_after);
}

// ---------------------------------------------------------------------------
// Malformed persistence + backend error injection
// ---------------------------------------------------------------------------

/// Fail `load_hard_state` while delegating other operations to memory.
#[derive(Default)]
struct FailHardStateLoad(MemoryStorage);

impl HardStateStore for FailHardStateLoad {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        Err(StorageError::Backend("injected load failure".into()))
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.0.save_hard_state(state)
    }
}

impl LogStore for FailHardStateLoad {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        self.0.first_index()
    }
    fn last_index(&self) -> Result<LogIndex, StorageError> {
        self.0.last_index()
    }
    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        self.0.read(index)
    }
    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        self.0.read_from(from)
    }
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        self.0.append(entries)
    }
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        self.0.truncate_suffix(from)
    }
    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        self.0.purge_prefix(through)
    }
}

impl SnapshotStore for FailHardStateLoad {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.0.save_snapshot(snapshot)
    }
    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        self.0.load_snapshot()
    }
}

/// Fail every append after `allow` successful appends.
struct FailAppendAfter {
    inner: MemoryStorage,
    allow: usize,
    seen: Mutex<usize>,
}

impl FailAppendAfter {
    fn new(allow: usize) -> Self {
        Self {
            inner: MemoryStorage::default(),
            allow,
            seen: Mutex::new(0),
        }
    }
}

impl HardStateStore for FailAppendAfter {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        self.inner.load_hard_state()
    }
    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.inner.save_hard_state(state)
    }
}

impl LogStore for FailAppendAfter {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.first_index()
    }
    fn last_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.last_index()
    }
    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        self.inner.read(index)
    }
    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        self.inner.read_from(from)
    }
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        let mut seen = self.seen.lock().unwrap();
        *seen += 1;
        if *seen > self.allow {
            Err(StorageError::Backend("injected append failure".into()))
        } else {
            self.inner.append(entries)
        }
    }
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        self.inner.truncate_suffix(from)
    }
    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        self.inner.purge_prefix(through)
    }
}

impl SnapshotStore for FailAppendAfter {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.inner.save_snapshot(snapshot)
    }
    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        self.inner.load_snapshot()
    }
}

#[test]
fn recover_surfaces_storage_error_when_hard_state_unreadable() {
    let err = match RaftDriver::recover(
        NodeId(1),
        [NodeId(1)],
        config(),
        TrackedKv::default(),
        Box::new(FailHardStateLoad::default()),
    ) {
        Err(err) => err,
        Ok(_) => panic!("expected recover to fail on hard-state load"),
    };
    assert!(
        matches!(err, DriverError::Storage(StorageError::Backend(_))),
        "expected backend load failure, got {err:?}"
    );
}

#[test]
fn recover_rejects_corrupt_snapshot_bytes() {
    let mut storage = MemoryStorage::default();
    storage
        .save_hard_state(&HardState {
            current_term: Term(1),
            voted_for: Some(NodeId(1)),
        })
        .unwrap();
    storage
        .save_snapshot(&Snapshot {
            meta: SnapshotMeta {
                last_included: LogId::new(Term(1), LogIndex(1)),
                membership: Membership {
                    voters: vec![NodeId(1)],
                    voters_outgoing: vec![],
                    learners: vec![],
                },
            },
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        })
        .unwrap();

    let err = match RaftDriver::recover(
        NodeId(1),
        [NodeId(1)],
        config(),
        TrackedKv::default(),
        Box::new(storage),
    ) {
        Err(err) => err,
        Ok(_) => panic!("expected recover to fail on corrupt snapshot"),
    };
    assert!(
        matches!(err, DriverError::Restore(_)),
        "expected restore failure, got {err:?}"
    );
}

#[test]
fn replaying_corrupt_command_returns_apply_error() {
    let mut storage = MemoryStorage::default();
    storage
        .save_hard_state(&HardState {
            current_term: Term(1),
            voted_for: Some(NodeId(1)),
        })
        .unwrap();
    storage
        .append(&[
            LogEntry {
                term: Term(1),
                index: LogIndex(1),
                payload: EntryPayload::Noop,
            },
            LogEntry {
                term: Term(1),
                index: LogIndex(2),
                payload: EntryPayload::Command(vec![0xDE, 0xAD]),
            },
        ])
        .unwrap();

    let mut d = RaftDriver::recover(
        NodeId(1),
        [NodeId(1)],
        config(),
        TrackedKv::default(),
        Box::new(storage),
    )
    .unwrap();
    let err = match d.campaign() {
        Err(err) => err,
        Ok(_) => panic!("expected apply failure for corrupt command"),
    };
    assert!(
        matches!(err, DriverError::Apply { .. } | DriverError::Codec(_)),
        "expected apply/codec failure for corrupt command, got {err:?}"
    );
}

#[test]
fn storage_append_failure_surfaces_as_fatal_storage_error() {
    // Allow the leader election no-op, then fail the first client append.
    let storage = FailAppendAfter::new(1);
    let node = RaftNode::new(NodeId(1), [NodeId(1)], config());
    let mut d = RaftDriver::with_storage(node, TrackedKv::default(), Box::new(storage));
    d.campaign().unwrap();
    let err = d
        .propose(&KvCommand::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .unwrap_err();
    assert!(
        matches!(err, DriverError::Storage(StorageError::Backend(_))),
        "expected append backend failure, got {err:?}"
    );
}
