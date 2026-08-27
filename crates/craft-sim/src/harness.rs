//! Deterministic, seed-reproducible multi-node Raft simulator (testing-strategy).
//!
//! Drives many [`RaftNode`]s through a virtual network with injectable
//! latency, message loss, partitions, and crashes. Every step asserts core
//! safety invariants, so a single failing seed reproduces the exact schedule
//! that broke them.

use std::collections::{BTreeMap, BTreeSet};

use craft_core::{Config, Output, RaftNode, ReadId};
use craft_proto::{LogIndex, Membership, NodeId, RaftRpc, RaftRpcReply};

use crate::rng::Rng;

#[derive(Clone)]
enum Wire {
    Req(RaftRpc),
    Rep(RaftRpcReply),
}

struct Envelope {
    from: NodeId,
    to: NodeId,
    wire: Wire,
    at: u64,
}

/// Network fault profile. Defaults to a perfectly reliable network.
#[derive(Debug, Clone)]
pub struct Fault {
    /// Percent chance `[0, 100]` a message is dropped when sent.
    pub drop_percent: u64,
    /// Maximum delivery latency in ticks (`>= 1`).
    pub max_latency: u64,
}

impl Default for Fault {
    fn default() -> Self {
        Self {
            drop_percent: 0,
            max_latency: 1,
        }
    }
}

/// A running cluster simulation.
pub struct Cluster {
    ids: Vec<NodeId>,
    nodes: BTreeMap<NodeId, RaftNode>,
    queue: Vec<Envelope>,
    now: u64,
    rng: Rng,
    fault: Fault,
    partition: Option<Vec<BTreeSet<NodeId>>>,
    down: BTreeSet<NodeId>,

    applied: BTreeMap<NodeId, Vec<(LogIndex, Vec<u8>)>>,
    committed: BTreeMap<u64, Vec<u8>>,
    leaders_per_term: BTreeMap<u64, BTreeSet<NodeId>>,

    reads_ready: BTreeMap<u64, LogIndex>,
    reads_failed: BTreeSet<u64>,
    snapshots_loaded: BTreeMap<NodeId, LogIndex>,
}

impl Cluster {
    /// Build an `n`-node cluster with ids `1..=n`, all voters, seeded.
    #[must_use]
    pub fn new(n: u64, seed: u64) -> Self {
        let all: Vec<u64> = (1..=n).collect();
        Self::with_membership(n, &all, seed)
    }

    /// Build a cluster of `objects` node processes (ids `1..=objects`) whose
    /// initial voting set is `voters`; the remaining nodes exist as
    /// followers and can be added later via [`Cluster::change_membership`].
    #[must_use]
    pub fn with_membership(objects: u64, voters: &[u64], seed: u64) -> Self {
        assert!(objects >= 1, "cluster needs at least one node");
        let ids: Vec<NodeId> = (1..=objects).map(NodeId).collect();
        let config = Config {
            election_timeout_min: 5,
            election_timeout_max: 10,
            heartbeat_interval: 1,
            seed,
            ..Default::default()
        };
        let membership = Membership {
            voters: voters.iter().map(|n| NodeId(*n)).collect(),
            voters_outgoing: Vec::new(),
            learners: Vec::new(),
        };
        let nodes = ids
            .iter()
            .map(|id| {
                (
                    *id,
                    RaftNode::with_membership(*id, membership.clone(), config.clone()),
                )
            })
            .collect();
        Self {
            ids,
            nodes,
            queue: Vec::new(),
            now: 0,
            rng: Rng::new(seed ^ 0xD1B5_4A32_D192_ED03),
            fault: Fault::default(),
            partition: None,
            down: BTreeSet::new(),
            applied: BTreeMap::new(),
            committed: BTreeMap::new(),
            leaders_per_term: BTreeMap::new(),
            reads_ready: BTreeMap::new(),
            reads_failed: BTreeSet::new(),
            snapshots_loaded: BTreeMap::new(),
        }
    }

    /// Set the network fault profile.
    pub fn set_fault(&mut self, fault: Fault) {
        self.fault = fault;
    }

    /// Partition the network into disjoint groups (ids given by number).
    pub fn partition(&mut self, groups: &[&[u64]]) {
        let groups = groups
            .iter()
            .map(|g| g.iter().map(|n| NodeId(*n)).collect::<BTreeSet<_>>())
            .collect();
        self.partition = Some(groups);
    }

    /// Isolate a single node from the rest.
    pub fn isolate(&mut self, id: u64) {
        let rest: Vec<u64> = self.ids.iter().map(|n| n.0).filter(|n| *n != id).collect();
        self.partition(&[&[id], &rest]);
    }

    /// Heal all partitions.
    pub fn heal(&mut self) {
        self.partition = None;
    }

    /// The node that currently believes itself leader with the highest term.
    #[must_use]
    pub fn leader(&self) -> Option<u64> {
        self.leader_node().map(|n| n.0)
    }

    fn leader_node(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.is_leader() && !self.down.contains(&n.id()))
            .max_by_key(|n| n.current_term().0)
            .map(RaftNode::id)
    }

    /// Commit index reported by node `id`.
    #[must_use]
    pub fn commit_index(&self, id: u64) -> LogIndex {
        self.nodes[&NodeId(id)].commit_index()
    }

    /// Current term at node `id`.
    #[must_use]
    pub fn term(&self, id: u64) -> u64 {
        self.nodes[&NodeId(id)].current_term().0
    }

    /// Node ids in the cluster.
    #[must_use]
    pub fn ids(&self) -> Vec<u64> {
        self.ids.iter().map(|n| n.0).collect()
    }

    /// Commands applied by node `id`, in order.
    #[must_use]
    pub fn applied(&self, id: u64) -> Vec<Vec<u8>> {
        self.applied
            .get(&NodeId(id))
            .map(|v| v.iter().map(|(_, c)| c.clone()).collect())
            .unwrap_or_default()
    }

    /// Number of distinct committed indices observed cluster-wide.
    #[must_use]
    pub fn committed_count(&self) -> usize {
        self.committed.len()
    }

    /// Propose a command via the current leader. Returns `false` if there is
    /// no leader to accept it.
    pub fn propose(&mut self, command: Vec<u8>) -> bool {
        let Some(id) = self.leader_node() else {
            return false;
        };
        let outs = {
            let node = self.nodes.get_mut(&id).expect("leader exists");
            if node.propose(command).is_err() {
                return false;
            }
            node.take_outputs()
        };
        self.process_outputs(id, outs);
        true
    }

    /// Voting set as seen by node `id` (sorted).
    #[must_use]
    pub fn voters(&self, id: u64) -> Vec<u64> {
        self.nodes[&NodeId(id)]
            .voters()
            .into_iter()
            .map(|n| n.0)
            .collect()
    }

    /// Compact the current leader's log up to its last-applied index (Raft §7),
    /// installing a snapshot with opaque bytes. Returns `false` if there is no
    /// leader or nothing new to compact.
    pub fn compact_leader(&mut self) -> bool {
        let Some(id) = self.leader_node() else {
            return false;
        };
        let node = self.nodes.get_mut(&id).expect("leader exists");
        let up_to = node.last_applied();
        node.compact(up_to, vec![0xAB])
    }

    /// Snapshot boundary index at node `id` (0 if it holds no snapshot).
    #[must_use]
    pub fn snapshot_index(&self, id: u64) -> LogIndex {
        self.nodes[&NodeId(id)].snapshot_index()
    }

    /// The index a node last installed a leader snapshot at, if any.
    #[must_use]
    pub fn snapshot_loaded(&self, id: u64) -> Option<LogIndex> {
        self.snapshots_loaded.get(&NodeId(id)).copied()
    }

    /// Issue a linearizable ReadIndex read (read-consistency) via the current leader.
    /// Returns `false` if there is no leader to accept it.
    pub fn read_index(&mut self, id: u64) -> bool {
        let Some(node_id) = self.leader_node() else {
            return false;
        };
        let outs = {
            let node = self.nodes.get_mut(&node_id).expect("leader exists");
            if node.read_index(ReadId(id)).is_err() {
                return false;
            }
            node.take_outputs()
        };
        self.process_outputs(node_id, outs);
        true
    }

    /// The confirmed read index for read `id`, once it has completed.
    #[must_use]
    pub fn read_ready(&self, id: u64) -> Option<LogIndex> {
        self.reads_ready.get(&id).copied()
    }

    /// Whether read `id` failed (leadership changed before it confirmed).
    #[must_use]
    pub fn read_failed(&self, id: u64) -> bool {
        self.reads_failed.contains(&id)
    }

    /// Start a joint-consensus membership change via the current leader.
    /// Returns `false` if there is no leader or a change is already in flight.
    pub fn change_membership(&mut self, new_voters: &[u64], learners: &[u64]) -> bool {
        let Some(id) = self.leader_node() else {
            return false;
        };
        let voters: Vec<NodeId> = new_voters.iter().map(|n| NodeId(*n)).collect();
        let learners: Vec<NodeId> = learners.iter().map(|n| NodeId(*n)).collect();
        let outs = {
            let node = self.nodes.get_mut(&id).expect("leader exists");
            if node.propose_membership(voters, learners).is_err() {
                return false;
            }
            node.take_outputs()
        };
        self.process_outputs(id, outs);
        true
    }

    /// Run for `steps` ticks.
    pub fn run(&mut self, steps: u64) {
        for _ in 0..steps {
            self.step();
        }
    }

    /// Step until a leader exists or `max` ticks elapse. Returns whether a
    /// leader was found.
    pub fn run_until_leader(&mut self, max: u64) -> bool {
        for _ in 0..max {
            self.step();
            if self.leader().is_some() {
                return true;
            }
        }
        self.leader().is_some()
    }

    // ---- internals -------------------------------------------------------

    fn step(&mut self) {
        self.now += 1;

        let mut due = Vec::new();
        let mut keep = Vec::new();
        for env in std::mem::take(&mut self.queue) {
            if env.at <= self.now {
                due.push(env);
            } else {
                keep.push(env);
            }
        }
        self.queue = keep;
        self.shuffle(&mut due);

        for env in due {
            if !self.connected(env.from, env.to) {
                continue; // partition/crash changed mid-flight
            }
            let outs = {
                let node = self.nodes.get_mut(&env.to).expect("target exists");
                match env.wire {
                    Wire::Req(r) => node.receive(env.from, r),
                    Wire::Rep(r) => node.receive_reply(env.from, r),
                }
                node.take_outputs()
            };
            self.process_outputs(env.to, outs);
        }

        for id in self.ids.clone() {
            if self.down.contains(&id) {
                continue;
            }
            let outs = {
                let node = self.nodes.get_mut(&id).expect("node exists");
                node.tick();
                node.take_outputs()
            };
            self.process_outputs(id, outs);
        }

        self.check_election_safety();
    }

    fn process_outputs(&mut self, id: NodeId, outs: Vec<Output>) {
        for o in outs {
            match o {
                Output::Send(to, rpc) => self.enqueue(id, to, Wire::Req(rpc)),
                Output::Reply(to, rep) => self.enqueue(id, to, Wire::Rep(rep)),
                Output::Apply(c) => {
                    // Agreement: no two nodes may apply different commands at
                    // the same index, ever.
                    if let Some(prev) = self.committed.get(&c.index.0) {
                        assert_eq!(
                            prev, &c.command,
                            "divergent commit at index {} (node {})",
                            c.index.0, id.0
                        );
                    } else {
                        self.committed.insert(c.index.0, c.command.clone());
                    }
                    // Per-node applied indices must strictly increase.
                    let log = self.applied.entry(id).or_default();
                    if let Some((last, _)) = log.last() {
                        assert!(
                            c.index.0 > last.0,
                            "node {} applied index {} after {}",
                            id.0,
                            c.index.0,
                            last.0
                        );
                    }
                    log.push((c.index, c.command));
                }
                Output::RoleChanged(_) => {}
                Output::ReadReady { id: read_id, index } => {
                    // Linearizability: the read index is never beyond what the
                    // confirming leader has actually committed.
                    let committed = self.nodes[&id].commit_index();
                    assert!(
                        index <= committed,
                        "read {} index {} exceeds node {}'s commit index {}",
                        read_id.0,
                        index.0,
                        id.0,
                        committed.0
                    );
                    self.reads_ready.insert(read_id.0, index);
                }
                Output::ReadFailed { id: read_id } => {
                    self.reads_failed.insert(read_id.0);
                }
                Output::LoadSnapshot { index, .. } => {
                    self.snapshots_loaded.insert(id, index);
                }
                Output::CatalogApplied { .. } => {}
                Output::SagaJournalApplied { .. } => {}
            }
        }
    }

    fn enqueue(&mut self, from: NodeId, to: NodeId, wire: Wire) {
        if !self.connected(from, to) {
            return;
        }
        if self.fault.drop_percent > 0 && self.rng.range(0, 99) < self.fault.drop_percent {
            return;
        }
        let latency = self.rng.range(1, self.fault.max_latency.max(1));
        self.queue.push(Envelope {
            from,
            to,
            wire,
            at: self.now + latency,
        });
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        if self.down.contains(&a) || self.down.contains(&b) {
            return false;
        }
        match &self.partition {
            None => true,
            Some(groups) => groups.iter().any(|g| g.contains(&a) && g.contains(&b)),
        }
    }

    fn check_election_safety(&mut self) {
        for id in &self.ids {
            let node = &self.nodes[id];
            if node.is_leader() {
                let term = node.current_term().0;
                let set = self.leaders_per_term.entry(term).or_default();
                set.insert(*id);
                assert!(
                    set.len() <= 1,
                    "election safety violated: {:?} both led term {}",
                    set,
                    term
                );
            }
        }
    }

    fn shuffle<T>(&mut self, v: &mut [T]) {
        let n = v.len();
        for i in (1..n).rev() {
            let j = (self.rng.next_u64() % (i as u64 + 1)) as usize;
            v.swap(i, j);
        }
    }
}
