//! The pure Raft consensus state machine.
//!
//! [`RaftNode`] performs no I/O: it consumes events (`tick`, `receive`,
//! `receive_reply`, `propose`, `propose_membership`, `read_index`) and
//! accumulates [`Output`] effects that an outer runtime executes (send
//! messages, apply commands, complete reads). Time is logical — the runtime
//! calls [`RaftNode::tick`] once per logical unit — so a given seed replays
//! deterministically (testing-strategy, architecture-style).
//!
//! * Membership uses **joint consensus** (membership-early): a change appends a
//!   transitional `C_old,new` entry that requires majorities in *both* voter
//!   sets; once it commits, the leader appends the final `C_new`.
//! * Elections use **Pre-Vote** (Raft thesis §9.6) so isolated nodes cannot
//!   disrupt a live leader by inflating terms.
//! * Linearizable reads use **`ReadIndex`** (read-consistency): the leader confirms it is
//!   still leader via a heartbeat round to a quorum before serving the read.

use std::collections::{BTreeMap, BTreeSet};

use trembita_proto::{
    AppendEntries, AppendEntriesReply, CatalogCommand, EntryPayload, InstallSnapshot,
    InstallSnapshotReply, LogEntry, LogId, LogIndex, Membership, NodeId,
    QueueAutoscalePolicyCommand, RaftRpc, RaftRpcReply, RequestVote, RequestVoteReply, Round,
    SagaJournalCommand, Term, TwoPhaseAbortCommand, TwoPhaseJournalCommand, TwoPhasePrepareCommand,
};

use crate::config::Configuration;
use crate::failure_detector::{
    AckWindowLiveness, FailureDetectorKind, PhiAccrualLiveness, ReachabilityConfig,
};
use crate::log::Log;
use crate::rng::Rng;

/// The role a node currently plays in its term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive; redirects clients and waits for heartbeats.
    Follower,
    /// Running a pre-vote round (no term bump yet) to avoid disrupting a
    /// live leader (Raft thesis §9.6).
    PreCandidate,
    /// Seeking votes for a new term.
    Candidate,
    /// Elected; replicates the log and serves clients.
    Leader,
}

/// Timing and determinism configuration, in logical ticks.
#[derive(Debug, Clone)]
pub struct Config {
    /// Lower bound of the randomized election timeout (ticks).
    pub election_timeout_min: u64,
    /// Upper bound of the randomized election timeout (ticks).
    pub election_timeout_max: u64,
    /// Ticks between leader heartbeats.
    pub heartbeat_interval: u64,
    /// Seed mixed with the node id for deterministic timeout jitter.
    pub seed: u64,
    /// Leader-side reachability tuning (reachability tuning).
    pub reachability: ReachabilityConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            election_timeout_min: 10,
            election_timeout_max: 20,
            heartbeat_interval: 3,
            seed: 0,
            reachability: ReachabilityConfig::default(),
        }
    }
}

/// A committed application command ready to apply to the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Log index of the command.
    pub index: LogIndex,
    /// The application-encoded command bytes.
    pub command: Vec<u8>,
}

/// Client-supplied token identifying a linearizable read request (read-consistency).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadId(pub u64);

/// An effect produced by the core for the runtime to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Send a request RPC to a peer.
    Send(NodeId, RaftRpc),
    /// Reply to a peer's request RPC.
    Reply(NodeId, RaftRpcReply),
    /// A committed command to apply, in index order.
    Apply(Committed),
    /// The node changed role (useful for observability and tests).
    RoleChanged(Role),
    /// A `ReadIndex` read is safe to serve: the state machine at `index` (or
    /// later) reflects everything committed before the request (read-consistency).
    ReadReady {
        /// The client's read token.
        id: ReadId,
        /// The confirmed read index.
        index: LogIndex,
    },
    /// A pending read could not be honored (leadership was lost); retry it
    /// against the new leader.
    ReadFailed {
        /// The client's read token.
        id: ReadId,
    },
    /// Load a snapshot installed from the leader into the application state
    /// machine, replacing all state through `index` (Raft §7).
    LoadSnapshot {
        /// Last log index the snapshot includes.
        index: LogIndex,
        /// Opaque application snapshot bytes.
        data: Vec<u8>,
    },
    /// A committed catalog metadata entry (dynamic catalog; not applied to the user SM).
    CatalogApplied {
        /// Log index of the catalog entry.
        index: LogIndex,
        /// Catalog command committed at `index`.
        command: CatalogCommand,
    },
    /// A committed saga journal entry (Meta-Raft saga journal; not applied to the user SM).
    SagaJournalApplied {
        /// Log index of the saga journal entry.
        index: LogIndex,
        /// Saga journal command committed at `index`.
        command: SagaJournalCommand,
    },
    /// A committed durable 2PC prepare entry (not applied to the user SM).
    TwoPhasePrepareApplied {
        /// Log index of the prepare entry.
        index: LogIndex,
        /// Prepare command committed at `index`.
        command: TwoPhasePrepareCommand,
    },
    /// A committed durable 2PC abort entry (not applied to the user SM).
    TwoPhaseAbortApplied {
        /// Log index of the abort entry.
        index: LogIndex,
        /// Abort command committed at `index`.
        command: TwoPhaseAbortCommand,
    },
    /// A committed 2PC client journal entry (not applied to the user SM).
    TwoPhaseJournalApplied {
        /// Log index of the journal entry.
        index: LogIndex,
        /// Journal command committed at `index`.
        command: TwoPhaseJournalCommand,
    },
    /// A committed queue autoscale policy entry (Meta-Raft; not applied to the user SM).
    QueueAutoscalePolicyApplied {
        /// Log index of the policy entry.
        index: LogIndex,
        /// Policy command committed at `index`.
        command: QueueAutoscalePolicyCommand,
    },
}

/// Returned by [`RaftNode::propose`] / [`RaftNode::read_index`] when the node
/// is not the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotLeader {
    /// Best-known current leader, if any, for client redirection.
    pub leader: Option<NodeId>,
}

/// Why a membership change could not be started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipError {
    /// This node is not the leader.
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// A previous membership change has not finished committing yet.
    InProgress,
    /// The requested configuration has no voters.
    EmptyVoters,
}

/// Why a catalog metadata change could not be started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogProposeError {
    /// This node is not the leader.
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
}

/// A batch of durable state changes an outer runtime must fsync **before**
/// acting on any network effect drained from the same step (Raft §5.1–§5.3):
/// a follower persists appended entries before ack'ing them, and a node
/// persists its term/vote before replying to a vote. Produced by
/// [`RaftNode::take_persist`]; it is the delta since the previous call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Persist {
    /// Current term to record in the hard state.
    pub term: Term,
    /// Vote cast in `term`, to record in the hard state.
    pub voted_for: Option<NodeId>,
    /// Whether the hard state (`term`/`voted_for`) actually changed and must be
    /// written; `false` means only the log changed this step.
    pub hard_state_dirty: bool,
    /// When set, the persisted log suffix at indices `>= from` must be
    /// truncated before `entries` are appended (conflict resolution, Raft §5.3).
    pub truncate_from: Option<LogIndex>,
    /// Entries to append after any truncation (ascending, contiguous).
    pub entries: Vec<LogEntry>,
}

/// A read-only view of this node's most recent snapshot (Raft §7): its
/// boundary `(term, index)`, the configuration in effect there, and the opaque
/// application bytes. Returned by [`RaftNode::stored_snapshot`] so a runtime can
/// persist the snapshot durably and purge the compacted log prefix (backlog
/// A6), and fed back to [`RaftNode::restore_with_snapshot`] on restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotState {
    /// `(term, index)` of the last log entry the snapshot includes.
    pub last_included: LogId,
    /// Cluster configuration at the snapshot boundary (its config entry may
    /// have been compacted out of the log, so it travels with the snapshot).
    pub membership: Membership,
    /// Opaque, application-encoded state-machine bytes.
    pub data: Vec<u8>,
}

/// A `ReadIndex` request awaiting leadership confirmation and apply catch-up.
#[derive(Debug, Clone)]
struct PendingRead {
    id: ReadId,
    index: LogIndex,
    round: Round,
    acks: BTreeSet<NodeId>,
}

/// The most recent snapshot this node holds — enough to ship to a lagging
/// follower and to recover the configuration after log compaction (Raft §7).
#[derive(Debug, Clone)]
struct StoredSnapshot {
    last_index: LogIndex,
    last_term: Term,
    membership: Membership,
    data: Vec<u8>,
}

/// A single Raft participant: a deterministic, I/O-free state machine.
#[derive(Debug, Clone)]
pub struct RaftNode {
    id: NodeId,
    initial: Membership,
    config: Config,

    // Persistent state (runtime is responsible for durability).
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // Durability watermarks (B4): the term/vote last handed to the storage
    // adapter and the lowest log index changed since then, so `take_persist`
    // can emit just the delta an outer runtime must fsync before acting on any
    // network effect from the same step (Raft §5.1–§5.3).
    persisted_term: Term,
    persisted_vote: Option<NodeId>,
    log_dirty_from: Option<LogIndex>,

    // Volatile state.
    role: Role,
    leader_id: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,

    // Candidate state.
    votes: BTreeSet<NodeId>,

    // Leader state.
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    sent_upper: BTreeMap<NodeId, LogIndex>,
    heartbeat_round: Round,
    pending_reads: Vec<PendingRead>,
    snapshot: Option<StoredSnapshot>,

    // Failure detection (liveness-vs-membership liveness): the `logical_clock` tick at which
    // each peer last acked an AppendEntries. Only the leader populates this (it
    // is the only role that solicits acks); it underpins `reachable`, a liveness
    // signal distinct from committed voter membership, so crash detection need
    // not wait for a `ConfChange`.
    last_ack_clock: BTreeMap<NodeId, u64>,
    ack_liveness: AckWindowLiveness,
    phi_liveness: PhiAccrualLiveness,

    // Leader lease (read-consistency lease reads): the leader may serve a read locally,
    // with no fresh quorum round, while it holds a valid lease. The lease is
    // extended to `lease_round_clock + lease_ticks` whenever a quorum acks the
    // heartbeat round broadcast at `lease_round_clock`; `lease_acks` accumulates
    // the acks for the current `lease_round`.
    lease_round: Round,
    lease_round_clock: u64,
    lease_acks: BTreeSet<NodeId>,
    lease_expiry: u64,

    // Timing (logical ticks).
    elapsed: u64,
    heartbeat_elapsed: u64,
    election_timeout: u64,
    /// Monotonic logical clock (never reset): the tick count since construction,
    /// used as the time base for the leader lease.
    logical_clock: u64,
    rng: Rng,

    outbox: Vec<Output>,
}

impl RaftNode {
    /// Create a node whose initial voting set is `members` (including `id`).
    #[must_use]
    pub fn new(id: NodeId, members: impl IntoIterator<Item = NodeId>, config: Config) -> Self {
        let mut voters: Vec<NodeId> = members.into_iter().collect();
        voters.sort();
        voters.dedup();
        let membership = Membership {
            voters,
            voters_outgoing: Vec::new(),
            learners: Vec::new(),
        };
        Self::with_membership(id, membership, config)
    }

    /// Create a node with an explicit initial [`Membership`] (voters +
    /// learners), used to bootstrap clusters that grow from a subset.
    #[must_use]
    pub fn with_membership(id: NodeId, membership: Membership, config: Config) -> Self {
        let mut rng = Rng::new(config.seed ^ id.0 ^ 0x9E37_79B9_7F4A_7C15);
        let election_timeout = rng.range(config.election_timeout_min, config.election_timeout_max);
        let phi_threshold = config.reachability.phi_threshold;
        Self {
            id,
            initial: membership,
            config,
            current_term: Term::ZERO,
            voted_for: None,
            log: Log::default(),
            persisted_term: Term::ZERO,
            persisted_vote: None,
            log_dirty_from: None,
            role: Role::Follower,
            leader_id: None,
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            sent_upper: BTreeMap::new(),
            heartbeat_round: Round::ZERO,
            pending_reads: Vec::new(),
            snapshot: None,
            last_ack_clock: BTreeMap::new(),
            ack_liveness: AckWindowLiveness::default(),
            phi_liveness: PhiAccrualLiveness::new(phi_threshold),
            lease_round: Round::ZERO,
            lease_round_clock: 0,
            lease_acks: BTreeSet::new(),
            lease_expiry: 0,
            elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            logical_clock: 0,
            rng,
            outbox: Vec::new(),
        }
    }

    /// Rebuild a node from durably persisted state after a restart (backlog
    /// B4). `term`/`voted_for` come from the stored `HardState` and `entries`
    /// are the stored log (ascending, contiguous from index 1 — no snapshot
    /// support yet). The node comes back as a [`Follower`](Role::Follower) with
    /// `commit_index`/`last_applied` at 0: it re-learns its commit index from
    /// the current leader (or re-derives it after winning an election) and the
    /// application state machine is rebuilt by replaying the recovered log.
    ///
    /// `members` is the bootstrap voter set, used only when the recovered log
    /// carries no membership entry (a cluster that never reconfigured).
    #[must_use]
    pub fn restore(
        id: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        config: Config,
        term: Term,
        voted_for: Option<NodeId>,
        entries: impl IntoIterator<Item = LogEntry>,
    ) -> Self {
        let mut node = Self::new(id, members, config);
        node.current_term = term;
        node.voted_for = voted_for;
        for entry in entries {
            node.log.push_entry(entry);
        }
        // Everything just loaded is already durable; start with a clean slate
        // so the first `take_persist` reports only post-restart changes.
        node.persisted_term = term;
        node.persisted_vote = voted_for;
        node.log_dirty_from = None;
        node
    }

    /// Rebuild a node from a durable snapshot plus the live log suffix after a
    /// restart (backlog A6). Used when the stored log was compacted: `snapshot`
    /// summarizes everything through `snapshot.last_included`, and `entries` are
    /// the remaining log entries (indices strictly greater than the boundary,
    /// ascending and contiguous).
    ///
    /// The application state machine must be restored from `snapshot.data`
    /// *before* the node is driven; the node comes back as a
    /// [`Follower`](Role::Follower) with `commit_index`/`last_applied` at the
    /// snapshot boundary (which is durably committed), then re-learns any higher
    /// commit index from the current leader and replays the suffix.
    #[must_use]
    pub fn restore_with_snapshot(
        id: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        config: Config,
        term: Term,
        voted_for: Option<NodeId>,
        snapshot: SnapshotState,
        entries: impl IntoIterator<Item = LogEntry>,
    ) -> Self {
        let mut node = Self::new(id, members, config);
        node.current_term = term;
        node.voted_for = voted_for;
        let last = snapshot.last_included;
        node.log.install_snapshot(last.index, last.term);
        node.snapshot = Some(StoredSnapshot {
            last_index: last.index,
            last_term: last.term,
            membership: snapshot.membership,
            data: snapshot.data,
        });
        for entry in entries {
            node.log.push_entry(entry);
        }
        // The snapshot boundary is durably committed and already reflected in
        // the restored state machine.
        node.commit_index = last.index;
        node.last_applied = last.index;
        node.persisted_term = term;
        node.persisted_vote = voted_for;
        node.log_dirty_from = None;
        node
    }

    // ---- Accessors -------------------------------------------------------

    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }
    /// Current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }
    /// Whether this node currently believes it is leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    /// Current term.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    /// Best-known leader.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    /// Highest committed index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    /// Highest applied index.
    #[must_use]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    /// Index of the last log entry.
    #[must_use]
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }
    /// Who this node voted for in the current term.
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    /// Term stored at `idx`, if present (for tests/introspection).
    #[must_use]
    pub fn term_at(&self, idx: LogIndex) -> Option<Term> {
        self.log.term_at(idx)
    }
    /// The active configuration. Prefers the last config entry still in the
    /// log, then the snapshot's configuration (its config entry may have been
    /// compacted), then the bootstrap configuration.
    #[must_use]
    pub(crate) fn configuration(&self) -> Configuration {
        let membership = self
            .log
            .last_membership()
            .map(|(_, m)| m)
            .or_else(|| self.snapshot.as_ref().map(|s| &s.membership))
            .unwrap_or(&self.initial);
        Configuration::from_membership(membership)
    }

    /// The committed membership as a wire [`Membership`] value.
    #[must_use]
    pub fn committed_membership(&self) -> trembita_proto::Membership {
        self.configuration().to_membership()
    }

    /// Highest index covered by this node's snapshot (0 if none).
    #[must_use]
    pub fn snapshot_index(&self) -> LogIndex {
        self.log.snapshot_index()
    }

    /// Applied entries not yet compacted into a snapshot.
    #[must_use]
    pub fn compactable_entries(&self) -> u64 {
        self.last_applied
            .0
            .saturating_sub(self.log.snapshot_index().0)
    }

    /// Estimated byte size of applied log entries not yet compacted.
    #[must_use]
    pub fn compactable_log_bytes(&self) -> u64 {
        self.log.bytes_up_to(self.last_applied)
    }

    /// The most recent snapshot this node holds (its boundary, configuration,
    /// and application bytes), or `None` if nothing has been compacted or
    /// installed. A runtime persists this via a `SnapshotStore` after a
    /// [`compact`](RaftNode::compact) or a leader-shipped install so it survives
    /// a restart (backlog A6).
    #[must_use]
    pub fn stored_snapshot(&self) -> Option<SnapshotState> {
        self.snapshot.as_ref().map(|s| SnapshotState {
            last_included: LogId::new(s.last_term, s.last_index),
            membership: s.membership.clone(),
            data: s.data.clone(),
        })
    }
    /// Live log entries from `from` through the last index (inclusive).
    #[must_use]
    pub fn log_entries_from(&self, from: LogIndex) -> Vec<LogEntry> {
        self.log.entries_from(from).to_vec()
    }

    /// The active voting set (sorted).
    #[must_use]
    pub fn voters(&self) -> Vec<NodeId> {
        self.configuration().voters()
    }

    /// Non-voting learners in the committed configuration (sorted).
    #[must_use]
    pub fn learners(&self) -> Vec<NodeId> {
        self.configuration().to_membership().learners
    }

    /// Highest replicated index reported by `peer` on this leader (zero if unknown).
    #[must_use]
    pub fn peer_match_index(&self, peer: NodeId) -> LogIndex {
        self.match_index
            .get(&peer)
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Collect `(peer, match_index)` for every configured peer except self.
    #[must_use]
    pub fn peer_match_indices(&self) -> BTreeMap<NodeId, LogIndex> {
        self.configuration()
            .members()
            .into_iter()
            .filter(|id| *id != self.id)
            .map(|id| (id, self.peer_match_index(id)))
            .collect()
    }
    /// Whether the active configuration is a joint (transitional) config.
    #[must_use]
    pub fn is_joint(&self) -> bool {
        self.configuration().is_joint()
    }

    /// Resolved reachability silence window in logical ticks.
    #[must_use]
    pub fn reachability_window_ticks(&self) -> u64 {
        self.config
            .reachability
            .window(self.config.election_timeout_max)
    }

    /// Drain accumulated effects. The runtime calls this after every event.
    #[must_use]
    pub fn take_outputs(&mut self) -> Vec<Output> {
        std::mem::take(&mut self.outbox)
    }

    /// Take the durable state delta accumulated since the previous call, or
    /// `None` if neither the hard state nor the log changed (backlog B4). The
    /// runtime persists the returned [`Persist`] *before* dispatching any
    /// [`Output`] from [`take_outputs`](RaftNode::take_outputs) for the same
    /// step, so a follower never ack's an entry it has not fsync'd and a node
    /// never reveals a vote it has not recorded (Raft §5.1–§5.3).
    #[must_use]
    pub fn take_persist(&mut self) -> Option<Persist> {
        let hard_state_dirty =
            self.current_term != self.persisted_term || self.voted_for != self.persisted_vote;
        let log_from = self.log_dirty_from.take();
        if !hard_state_dirty && log_from.is_none() {
            return None;
        }
        self.persisted_term = self.current_term;
        self.persisted_vote = self.voted_for;
        let (truncate_from, entries) = match log_from {
            // Never touch indices already sealed into a snapshot; clamp to the
            // first live index.
            Some(from) => {
                let from = LogIndex(from.0.max(self.log.snapshot_index().0 + 1));
                (Some(from), self.log.entries_from(from).to_vec())
            }
            None => (None, Vec::new()),
        };
        Some(Persist {
            term: self.current_term,
            voted_for: self.voted_for,
            hard_state_dirty,
            truncate_from,
            entries,
        })
    }

    // ---- Log mutation (durability-tracked) -------------------------------

    /// Lowest index whose entry changed; `take_persist` emits from here.
    fn mark_log_dirty(&mut self, from: LogIndex) {
        self.log_dirty_from = Some(match self.log_dirty_from {
            Some(cur) if cur.0 <= from.0 => cur,
            _ => from,
        });
    }

    /// Append a fresh entry and record it as dirty for persistence.
    fn log_append(&mut self, term: Term, payload: EntryPayload) -> LogIndex {
        let idx = self.log.append(term, payload);
        self.mark_log_dirty(idx);
        idx
    }

    /// Push a pre-built entry and record it as dirty for persistence.
    fn log_push(&mut self, entry: LogEntry) {
        let idx = entry.index;
        self.log.push_entry(entry);
        self.mark_log_dirty(idx);
    }

    /// Truncate the log suffix and record the cut point as dirty.
    fn log_truncate_from(&mut self, idx: LogIndex) {
        self.log.truncate_from(idx);
        self.mark_log_dirty(idx);
    }

    // ---- Configuration helpers ------------------------------------------

    fn config_index(&self) -> LogIndex {
        self.log
            .last_membership()
            .map_or(LogIndex::ZERO, |(idx, _)| idx)
    }

    fn is_voter(&self, id: NodeId) -> bool {
        self.configuration().is_voter(id)
    }

    fn peers(&self) -> Vec<NodeId> {
        self.configuration().peers(self.id)
    }

    /// Whether `acked` satisfies quorum in the current (possibly joint) config.
    fn quorum_ok(&self, acked: &BTreeSet<NodeId>) -> bool {
        self.configuration().has_quorum(acked)
    }

    fn quorum_of_votes(&self) -> bool {
        let votes = self.votes.clone();
        self.quorum_ok(&votes)
    }

    // ---- Events ----------------------------------------------------------

    /// Advance logical time by one tick (election / heartbeat timers).
    pub fn tick(&mut self) {
        self.logical_clock += 1;
        if self.role == Role::Leader {
            self.update_liveness();
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
        } else {
            self.elapsed += 1;
            if self.elapsed >= self.election_timeout {
                self.start_pre_election();
            }
        }
    }

    /// Force a real election immediately, skipping the pre-vote round (used
    /// for tests and leadership transfer, which bypass pre-vote by design).
    pub fn campaign(&mut self) {
        self.start_real_election();
    }

    /// Handle an inbound request RPC from `from`.
    pub fn receive(&mut self, from: NodeId, rpc: RaftRpc) {
        match rpc {
            RaftRpc::RequestVote(rv) => self.handle_request_vote(from, &rv),
            RaftRpc::AppendEntries(ae) => self.handle_append_entries(from, &ae),
            RaftRpc::InstallSnapshot(is) => self.handle_install_snapshot(from, is),
        }
    }

    /// Handle an inbound reply RPC from `from`.
    pub fn receive_reply(&mut self, from: NodeId, reply: RaftRpcReply) {
        let term = match &reply {
            RaftRpcReply::RequestVote(r) => r.term,
            RaftRpcReply::AppendEntries(r) => r.term,
            RaftRpcReply::InstallSnapshot(r) => r.term,
        };
        if term > self.current_term {
            self.become_follower(term);
            return;
        }
        match reply {
            RaftRpcReply::RequestVote(r) => self.handle_vote_reply(from, &r),
            RaftRpcReply::AppendEntries(r) => self.handle_append_reply(from, &r),
            RaftRpcReply::InstallSnapshot(r) => self.handle_snapshot_reply(from, &r),
        }
    }

    /// Propose a new command. Succeeds only on the leader; effects (log append
    /// and replication) are drained via [`RaftNode::take_outputs`].
    ///
    /// # Errors
    /// Returns [`NotLeader`] with a redirect hint if this node is not leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<LogIndex, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::Command(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Request a linearizable read (`ReadIndex`, read-consistency). The leader captures
    /// its commit index and confirms it still leads by a heartbeat round to a
    /// quorum; once confirmed and applied, an [`Output::ReadReady`] is emitted.
    /// If leadership is lost first, an [`Output::ReadFailed`] is emitted.
    ///
    /// # Errors
    /// Returns [`NotLeader`] with a redirect hint if this node is not leader.
    pub fn read_index(&mut self, id: ReadId) -> Result<(), NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id,
            });
        }
        // A fresh heartbeat round whose quorum of acks proves we still lead.
        self.broadcast_append();
        let round = self.heartbeat_round;
        let mut acks = BTreeSet::new();
        acks.insert(self.id);
        self.pending_reads.push(PendingRead {
            id,
            index: self.commit_index,
            round,
            acks,
        });
        self.try_complete_reads();
        Ok(())
    }

    /// Compact the log up to and including `up_to`, replacing that prefix with
    /// a snapshot whose application state is `data` (Raft §7). The runtime
    /// supplies `data` from its state machine after applying through `up_to`.
    ///
    /// Returns `false` if `up_to` is not a compactable applied index
    /// (`snapshot_index < up_to <= last_applied`).
    #[must_use]
    pub fn compact(&mut self, up_to: LogIndex, data: Vec<u8>) -> bool {
        if up_to <= self.log.snapshot_index() || up_to > self.last_applied {
            return false;
        }
        let Some(term) = self.log.term_at(up_to) else {
            return false;
        };
        let membership = self.membership_at(up_to);
        self.log.compact(up_to, term);
        self.snapshot = Some(StoredSnapshot {
            last_index: up_to,
            last_term: term,
            membership,
            data,
        });
        true
    }

    /// The configuration in effect at log index `idx`: the last membership
    /// entry at or before `idx`, else the snapshot's, else the bootstrap one.
    fn membership_at(&self, idx: LogIndex) -> Membership {
        for i in (self.log.snapshot_index().0 + 1..=idx.0).rev() {
            if let Some(EntryPayload::Membership(m)) = self.log.get(LogIndex(i)).map(|e| &e.payload)
            {
                return m.clone();
            }
        }
        self.snapshot
            .as_ref()
            .map_or_else(|| self.initial.clone(), |s| s.membership.clone())
    }

    /// Begin a joint-consensus membership change to `new_voters` (+ optional
    /// `learners`). Only the leader may call this, and only when no other
    /// change is in flight (membership-early).
    ///
    /// # Errors
    /// Returns [`MembershipError`] if not leader, a change is in progress, or
    /// the new voter set is empty.
    pub fn propose_membership(
        &mut self,
        new_voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<LogIndex, MembershipError> {
        if self.role != Role::Leader {
            return Err(MembershipError::NotLeader {
                leader: self.leader_id,
            });
        }
        let current = self.configuration();
        if current.is_joint() || self.config_index() > self.commit_index {
            return Err(MembershipError::InProgress);
        }
        let mut voters: Vec<NodeId> = new_voters.into_iter().collect();
        voters.sort();
        voters.dedup();
        if voters.is_empty() {
            return Err(MembershipError::EmptyVoters);
        }
        let mut learners: Vec<NodeId> = learners.into_iter().collect();
        learners.sort();
        learners.dedup();
        learners.retain(|l| !voters.contains(l));

        let joint = Membership {
            voters,
            voters_outgoing: current.voters(),
            learners,
        };
        let idx = self.log_append(self.current_term, EntryPayload::Membership(joint));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a catalog metadata entry to the log (group 0 only, dynamic catalog).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_catalog(
        &mut self,
        command: CatalogCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::Catalog(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a saga journal metadata entry to the log (group 0 only, Meta-Raft saga journal).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_saga_journal(
        &mut self,
        command: SagaJournalCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::SagaJournal(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a durable 2PC prepare entry to the log (any Raft group leader).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_two_phase_prepare(
        &mut self,
        command: TwoPhasePrepareCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::TwoPhasePrepare(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a durable 2PC abort entry to the log (any Raft group leader).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_two_phase_abort(
        &mut self,
        command: TwoPhaseAbortCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::TwoPhaseAbort(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a 2PC client journal metadata entry to the log (group 0 / Meta-Raft).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_two_phase_journal(
        &mut self,
        command: TwoPhaseJournalCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::TwoPhaseJournal(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a queue autoscale policy metadata entry to the log (Meta-Raft / group 0).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_queue_autoscale_policy(
        &mut self,
        command: QueueAutoscalePolicyCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(
            self.current_term,
            EntryPayload::QueueAutoscalePolicy(command),
        );
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    // ---- Role transitions ------------------------------------------------

    fn set_role(&mut self, role: Role) {
        if self.role != role {
            tracing::debug!(
                target: "trembita::raft",
                node = self.id.0,
                term = self.current_term.0,
                ?role,
                "raft role changed"
            );
            self.role = role;
            self.outbox.push(Output::RoleChanged(role));
        }
    }

    fn become_follower(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.votes.clear();
        self.fail_pending_reads();
        // Surrender the lease immediately on step-down: a follower must never
        // serve a lease read, and a stale lease could otherwise linger.
        self.lease_expiry = 0;
        self.lease_acks.clear();
        self.set_role(Role::Follower);
    }

    /// Pre-vote round: probe whether a real election could succeed *without*
    /// bumping our term, so an isolated/removed node cannot disrupt a live
    /// leader by forcing term inflation (Raft thesis §9.6).
    fn start_pre_election(&mut self) {
        if !self.is_voter(self.id) {
            self.reset_election_timer();
            return;
        }
        self.set_role(Role::PreCandidate);
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();

        if self.quorum_of_votes() {
            self.start_real_election();
            return;
        }

        // Advertise the term we *would* run in, without adopting it.
        let rv = RequestVote {
            term: self.current_term.next(),
            candidate_id: self.id,
            last_log: self.log.last_id(),
            pre_vote: true,
        };
        self.send_vote_requests(&rv);
    }

    fn start_real_election(&mut self) {
        if !self.is_voter(self.id) {
            self.reset_election_timer();
            return;
        }
        self.current_term = self.current_term.next();
        self.set_role(Role::Candidate);
        self.voted_for = Some(self.id);
        self.votes.clear();
        self.votes.insert(self.id);
        self.leader_id = None;
        self.reset_election_timer();

        if self.quorum_of_votes() {
            self.become_leader();
            return;
        }

        let rv = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log: self.log.last_id(),
            pre_vote: false,
        };
        self.send_vote_requests(&rv);
    }

    fn send_vote_requests(&mut self, rv: &RequestVote) {
        for p in self.configuration().voter_peers(self.id) {
            self.outbox
                .push(Output::Send(p, RaftRpc::RequestVote(rv.clone())));
        }
    }

    fn become_leader(&mut self) {
        self.set_role(Role::Leader);
        self.leader_id = Some(self.id);
        // A fresh term starts with no lease; it is earned once a heartbeat round
        // in this term is acked by a quorum (via `broadcast_append` below).
        self.lease_expiry = 0;
        let next = self.log.last_index().next();
        self.next_index.clear();
        self.match_index.clear();
        self.sent_upper.clear();
        // Reachability is earned afresh each term from this leader's own acks;
        // stale observations from a prior leadership must not count (liveness-vs-membership).
        self.last_ack_clock.clear();
        for p in self.peers() {
            self.next_index.insert(p, next);
            self.match_index.insert(p, LogIndex::ZERO);
        }
        // A no-op in the new term lets prior-term entries commit safely.
        self.log_append(self.current_term, EntryPayload::Noop);
        self.heartbeat_elapsed = 0;
        self.broadcast_append();
        self.maybe_advance_commit();
    }

    fn reset_election_timer(&mut self) {
        self.elapsed = 0;
        self.election_timeout = self.rng.range(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
    }

    // ---- RequestVote -----------------------------------------------------

    fn handle_request_vote(&mut self, from: NodeId, rv: &RequestVote) {
        if !self.is_voter(self.id) {
            self.reply_vote(from, false, rv.pre_vote);
            return;
        }

        let up_to_date = rv.last_log >= self.log.last_id();

        if rv.pre_vote {
            // Pre-vote never changes our term or vote. Refuse if we still
            // believe a leader is alive (heard from it within the min timeout),
            // which is what neutralizes disruptive removed servers.
            let leader_recent =
                self.leader_id.is_some() && self.elapsed < self.config.election_timeout_min;
            let granted = rv.term >= self.current_term && up_to_date && !leader_recent;
            self.reply_vote(from, granted, true);
            return;
        }

        if rv.term > self.current_term {
            self.become_follower(rv.term);
        }

        let mut granted = false;
        if rv.term >= self.current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(rv.candidate_id);
            if can_vote && up_to_date {
                granted = true;
                self.voted_for = Some(rv.candidate_id);
                self.reset_election_timer();
            }
        }
        self.reply_vote(from, granted, false);
    }

    fn reply_vote(&mut self, to: NodeId, vote_granted: bool, pre_vote: bool) {
        let reply = RequestVoteReply {
            term: self.current_term,
            vote_granted,
            pre_vote,
        };
        self.outbox
            .push(Output::Reply(to, RaftRpcReply::RequestVote(reply)));
    }

    fn handle_vote_reply(&mut self, from: NodeId, reply: &RequestVoteReply) {
        if reply.pre_vote {
            if self.role == Role::PreCandidate && reply.vote_granted {
                self.votes.insert(from);
                if self.quorum_of_votes() {
                    self.start_real_election();
                }
            }
            return;
        }
        if self.role != Role::Candidate || reply.term != self.current_term {
            return;
        }
        if reply.vote_granted {
            self.votes.insert(from);
            if self.quorum_of_votes() {
                self.become_leader();
            }
        }
    }

    // ---- AppendEntries ---------------------------------------------------

    fn handle_append_entries(&mut self, from: NodeId, ae: &AppendEntries) {
        if ae.term < self.current_term {
            self.reply_append(from, false, None, None, ae.round);
            return;
        }

        if ae.term > self.current_term {
            self.become_follower(ae.term);
        } else if self.role != Role::Follower {
            self.set_role(Role::Follower);
        }
        self.leader_id = Some(ae.leader_id);
        self.reset_election_timer();

        // Log-matching check on the entry preceding the new ones.
        if ae.prev_log.index.0 > 0 {
            match self.log.term_at(ae.prev_log.index) {
                None => {
                    let hint = self.log.last_index().next();
                    self.reply_append(from, false, Some(hint), None, ae.round);
                    return;
                }
                Some(t) if t != ae.prev_log.term => {
                    let first = self.log.first_index_of_term(t).unwrap_or(ae.prev_log.index);
                    self.reply_append(from, false, Some(first), Some(t), ae.round);
                    return;
                }
                _ => {}
            }
        }

        // Append, truncating on the first conflicting index.
        let mut idx = ae.prev_log.index;
        for entry in &ae.entries {
            idx = idx.next();
            match self.log.term_at(idx) {
                Some(t) if t == entry.term => {}
                Some(_) => {
                    self.log_truncate_from(idx);
                    self.log_push(LogEntry {
                        term: entry.term,
                        index: idx,
                        payload: entry.payload.clone(),
                    });
                }
                None => {
                    self.log_push(LogEntry {
                        term: entry.term,
                        index: idx,
                        payload: entry.payload.clone(),
                    });
                }
            }
        }

        if ae.leader_commit > self.commit_index {
            self.commit_index = ae.leader_commit.min(idx);
            self.apply_committed();
        }
        self.reply_append(from, true, None, None, ae.round);
    }

    fn reply_append(
        &mut self,
        to: NodeId,
        success: bool,
        conflict_index: Option<LogIndex>,
        conflict_term: Option<Term>,
        round: Round,
    ) {
        let reply = AppendEntriesReply {
            term: self.current_term,
            success,
            conflict_index,
            conflict_term,
            round,
        };
        self.outbox
            .push(Output::Reply(to, RaftRpcReply::AppendEntries(reply)));
    }

    fn handle_append_reply(&mut self, from: NodeId, reply: &AppendEntriesReply) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        if reply.success {
            let upper = self
                .sent_upper
                .get(&from)
                .copied()
                .unwrap_or(LogIndex::ZERO);
            let current = self
                .match_index
                .get(&from)
                .copied()
                .unwrap_or(LogIndex::ZERO);
            if upper > current {
                self.match_index.insert(from, upper);
            }
            self.next_index.insert(from, upper.next());
            // A successful ack is our freshest proof the peer is alive (liveness-vs-membership).
            self.last_ack_clock.insert(from, self.logical_clock);
            if self.config.reachability.detector == FailureDetectorKind::PhiAccrual {
                self.phi_liveness.record_heartbeat(from, self.logical_clock);
            }
            self.confirm_reads(from, reply.round);
            if reply.round >= self.lease_round {
                self.lease_acks.insert(from);
                self.maybe_extend_lease();
            }
            self.maybe_advance_commit();
            self.try_complete_reads();
        } else {
            let ni = if let Some(ci) = reply.conflict_index {
                LogIndex(ci.0.max(1))
            } else {
                let cur = self.next_index.get(&from).copied().unwrap_or(LogIndex(1)).0;
                LogIndex(cur.saturating_sub(1).max(1))
            };
            self.next_index.insert(from, ni);
            self.send_append(from);
        }
    }

    // ---- Replication helpers --------------------------------------------

    fn broadcast_append(&mut self) {
        // Each broadcast opens a new heartbeat round; acks echoing this round
        // (or later) confirm leadership for any read registered before it.
        self.heartbeat_round = self.heartbeat_round.next();
        // Open a fresh lease-confirmation round: a quorum of acks for it extends
        // the leader lease, measured from *now* (before any follower has even
        // received the heartbeat), which keeps the lease conservative (read-consistency).
        self.lease_round = self.heartbeat_round;
        self.lease_round_clock = self.logical_clock;
        self.lease_acks.clear();
        self.lease_acks.insert(self.id);
        self.maybe_extend_lease();
        for p in self.peers() {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let ni = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.log.last_index().next());
        // If the entries the follower needs have been compacted away, ship the
        // snapshot instead of an AppendEntries it could never match against.
        if ni.0 <= self.log.snapshot_index().0 && self.snapshot.is_some() {
            self.send_snapshot(peer);
            return;
        }
        let prev_index = LogIndex(ni.0.saturating_sub(1));
        let prev_term = self.log.term_at(prev_index).unwrap_or(Term::ZERO);
        let entries = self.log.entries_from(ni).to_vec();
        let upper = LogIndex(prev_index.0 + entries.len() as u64);
        self.sent_upper.insert(peer, upper);
        let ae = AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log: LogId::new(prev_term, prev_index),
            entries,
            leader_commit: self.commit_index,
            round: self.heartbeat_round,
        };
        self.outbox
            .push(Output::Send(peer, RaftRpc::AppendEntries(ae)));
    }

    fn send_snapshot(&mut self, peer: NodeId) {
        let Some(snap) = self.snapshot.as_ref() else {
            return;
        };
        let is = InstallSnapshot {
            term: self.current_term,
            leader_id: self.id,
            last_included: LogId::new(snap.last_term, snap.last_index),
            last_config: snap.membership.clone(),
            offset: 0,
            data: snap.data.clone(),
            done: true,
        };
        self.sent_upper.insert(peer, snap.last_index);
        self.outbox
            .push(Output::Send(peer, RaftRpc::InstallSnapshot(is)));
    }

    fn maybe_advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index().0;
        let mut new_commit = self.commit_index;
        for n in (self.commit_index.0 + 1)..=last {
            let idx = LogIndex(n);
            // Safety: a leader only commits entries from its own term directly.
            if self.log.term_at(idx) != Some(self.current_term) {
                continue;
            }
            let mut acked: BTreeSet<NodeId> = BTreeSet::new();
            acked.insert(self.id);
            for (peer, m) in &self.match_index {
                if m.0 >= n {
                    acked.insert(*peer);
                }
            }
            if self.quorum_ok(&acked) {
                new_commit = idx;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed();
            self.maybe_finalize_membership();
            self.maybe_step_down_if_removed();
            self.try_complete_reads();
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied.next();
            match self.log.get(next).map(|e| &e.payload) {
                Some(EntryPayload::Command(c)) => {
                    self.outbox.push(Output::Apply(Committed {
                        index: next,
                        command: c.clone(),
                    }));
                }
                Some(EntryPayload::Catalog(command)) => {
                    self.outbox.push(Output::CatalogApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::SagaJournal(command)) => {
                    self.outbox.push(Output::SagaJournalApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::TwoPhasePrepare(command)) => {
                    self.outbox.push(Output::TwoPhasePrepareApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::TwoPhaseAbort(command)) => {
                    self.outbox.push(Output::TwoPhaseAbortApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::TwoPhaseJournal(command)) => {
                    self.outbox.push(Output::TwoPhaseJournalApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::QueueAutoscalePolicy(command)) => {
                    self.outbox.push(Output::QueueAutoscalePolicyApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                _ => {}
            }
            self.last_applied = next;
        }
    }

    // ---- ReadIndex (read-consistency) --------------------------------------------

    /// Record that `from` acked a heartbeat at `round`, confirming leadership
    /// for every pending read registered no later than that round.
    fn confirm_reads(&mut self, from: NodeId, round: Round) {
        for r in &mut self.pending_reads {
            if round >= r.round {
                r.acks.insert(from);
            }
        }
    }

    /// Complete reads that are both leadership-confirmed (a quorum acked the
    /// read's round) and applied (`last_applied >= index`). A read is only
    /// served once the leader has committed an entry of its current term, so
    /// its commit index is authoritative.
    fn try_complete_reads(&mut self) {
        if self.role != Role::Leader || self.pending_reads.is_empty() {
            return;
        }
        if self.log.term_at(self.commit_index) != Some(self.current_term) {
            return;
        }
        let conf = self.configuration();
        let applied = self.last_applied;
        let mut ready = Vec::new();
        self.pending_reads.retain(|r| {
            if conf.has_quorum(&r.acks) && applied >= r.index {
                ready.push((r.id, r.index));
                false
            } else {
                true
            }
        });
        for (id, index) in ready {
            self.outbox.push(Output::ReadReady { id, index });
        }
    }

    fn fail_pending_reads(&mut self) {
        for r in std::mem::take(&mut self.pending_reads) {
            self.outbox.push(Output::ReadFailed { id: r.id });
        }
    }

    /// The leader lease duration, in logical ticks. Deliberately a fraction of
    /// the *minimum* election timeout so the lease is guaranteed to expire on
    /// the leader before any follower — which reset its election timer when it
    /// received the acked heartbeat — could time out and start an election.
    /// Halving leaves generous headroom for cross-node clock drift (read-consistency;
    /// this is why lease reads were originally deferred as "clock-sensitive").
    fn lease_ticks(&self) -> u64 {
        self.config.election_timeout_min / 2
    }

    /// Extend the leader lease if a quorum has acked the current lease round.
    /// Measured from when the round was broadcast (`lease_round_clock`), so the
    /// lease is always conservative relative to when followers last heard us.
    fn maybe_extend_lease(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        if self.configuration().has_quorum(&self.lease_acks) {
            let candidate = self.lease_round_clock.saturating_add(self.lease_ticks());
            if candidate > self.lease_expiry {
                self.lease_expiry = candidate;
            }
        }
    }

    /// Whether this node currently holds a valid leadership lease (leader, and
    /// within the lease window). Observability / test hook.
    #[must_use]
    pub fn lease_valid(&self) -> bool {
        self.role == Role::Leader && self.logical_clock < self.lease_expiry
    }

    /// Attempt a **lease read** (read-consistency): if this leader holds a valid lease and
    /// has committed an entry in its current term, return `Ok(Some(index))` — the
    /// read may be served by running `query` once the state machine has applied
    /// through `index`, with **no** `ReadIndex` round-trip. Returns `Ok(None)` when
    /// no valid lease is held (the caller should fall back to
    /// [`read_index`](Self::read_index)).
    ///
    /// # Errors
    /// Returns [`NotLeader`] with a redirect hint if this node is not the leader.
    pub fn lease_read(&self) -> Result<Option<LogIndex>, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id,
            });
        }
        // The commit index is only authoritative once an entry of the current
        // term has committed (leader completeness); until then, fall back.
        let authoritative = self.log.term_at(self.commit_index) == Some(self.current_term);
        if self.logical_clock < self.lease_expiry && authoritative {
            Ok(Some(self.commit_index))
        } else {
            Ok(None)
        }
    }

    /// The voters this node currently considers **reachable** — a liveness
    /// signal distinct from committed membership (liveness-vs-membership).
    ///
    /// On the leader this is itself plus every voter that acked an
    /// `AppendEntries` within the last `window` logical ticks; a voter silent for
    /// longer is treated as crashed/partitioned even though it is still a
    /// committed voter. A non-leader has no first-hand ack data, so it
    /// conservatively reports the full voter set and leaves crash detection to
    /// the leader (which is where reconcile runs anyway, supervisor-leader).
    ///
    /// `window` should comfortably exceed the heartbeat interval so a healthy
    /// follower is never flagged; [`reachable_now`](Self::reachable_now) applies
    /// a sensible default derived from the election timeout.
    #[must_use]
    pub fn reachable(&self, window: u64) -> Vec<NodeId> {
        let voters = self.configuration().voters();
        if self.role != Role::Leader {
            return voters;
        }
        let now = self.logical_clock;
        voters
            .into_iter()
            .filter(|&v| {
                v == self.id
                    || self
                        .last_ack_clock
                        .get(&v)
                        .is_some_and(|&acked| now.saturating_sub(acked) <= window)
            })
            .collect()
    }

    /// [`reachable`](Self::reachable) with configured window, hysteresis, or
    /// phi-accrual (reachability tuning). Updated every leader
    /// [`tick`](Self::tick).
    #[must_use]
    pub fn reachable_now(&self) -> Vec<NodeId> {
        let voters = self.configuration().voters();
        if self.role != Role::Leader {
            return voters;
        }
        let now = self.logical_clock;
        voters
            .into_iter()
            .filter(|&v| match self.config.reachability.detector {
                FailureDetectorKind::AckWindow => v == self.id || self.ack_liveness.is_reachable(v),
                FailureDetectorKind::PhiAccrual => {
                    v == self.id || self.phi_liveness.is_reachable(v, now)
                }
            })
            .collect()
    }

    /// Voters plus learners that acked recently — used for worker placement and
    /// auto-spawn on elastic joiners. Queue replication still uses
    /// [`reachable_now`](Self::reachable_now) (voters only).
    #[must_use]
    pub fn reachable_members_now(&self) -> Vec<NodeId> {
        let membership = self.configuration().to_membership();
        let mut members = membership.voters;
        members.extend(membership.learners);
        members.sort();
        members.dedup();
        if self.role != Role::Leader {
            return members;
        }
        let now = self.logical_clock;
        members
            .into_iter()
            .filter(|&id| self.is_member_reachable(id, now))
            .collect()
    }

    fn is_member_reachable(&self, peer: NodeId, now: u64) -> bool {
        if peer == self.id {
            return true;
        }
        let conf = self.configuration();
        match self.config.reachability.detector {
            FailureDetectorKind::AckWindow => {
                if conf.is_voter(peer) {
                    self.ack_liveness.is_reachable(peer)
                } else {
                    let window = self
                        .config
                        .reachability
                        .window(self.config.election_timeout_max);
                    self.last_ack_clock
                        .get(&peer)
                        .is_some_and(|&acked| now.saturating_sub(acked) <= window)
                }
            }
            FailureDetectorKind::PhiAccrual => self.phi_liveness.is_reachable(peer, now),
        }
    }

    fn update_liveness(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let voters = self.configuration().voters();
        let now = self.logical_clock;
        match self.config.reachability.detector {
            FailureDetectorKind::AckWindow => {
                let window = self
                    .config
                    .reachability
                    .window(self.config.election_timeout_max);
                let hysteresis = self
                    .config
                    .reachability
                    .hysteresis(self.config.election_timeout_min);
                self.ack_liveness.update(
                    now,
                    self.id,
                    &voters,
                    &self.last_ack_clock,
                    window,
                    hysteresis,
                );
            }
            FailureDetectorKind::PhiAccrual => {}
        }
    }

    // ---- Membership finalization (membership-early) ------------------------------

    /// Once a joint `C_old,new` entry commits, the leader appends the final
    /// `C_new` to leave the transitional configuration.
    fn maybe_finalize_membership(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let conf = self.configuration();
        let cfg_idx = self.config_index();
        if conf.is_joint() && cfg_idx.0 != 0 && cfg_idx <= self.commit_index {
            let final_config = Membership {
                voters: conf.voters(),
                voters_outgoing: Vec::new(),
                learners: conf.to_membership().learners,
            };
            self.log_append(self.current_term, EntryPayload::Membership(final_config));
            self.broadcast_append();
        }
    }

    /// If a committed, non-joint configuration excludes this leader, step down.
    fn maybe_step_down_if_removed(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let conf = self.configuration();
        if !conf.is_joint() && self.config_index() <= self.commit_index && !conf.is_voter(self.id) {
            self.become_follower(self.current_term);
            self.leader_id = None;
        }
    }

    // ---- InstallSnapshot (Raft §7) ---------------------------------------

    fn handle_install_snapshot(&mut self, from: NodeId, is: InstallSnapshot) {
        if is.term < self.current_term {
            self.reply_snapshot(from);
            return;
        }
        if is.term > self.current_term {
            self.become_follower(is.term);
        } else if self.role != Role::Follower {
            self.set_role(Role::Follower);
        }
        self.leader_id = Some(is.leader_id);
        self.reset_election_timer();

        let last = is.last_included;
        // Ignore snapshots we already cover; nothing to install.
        if last.index.0 <= self.log.snapshot_index().0 || last.index <= self.last_applied {
            self.reply_snapshot(from);
            return;
        }

        self.log.install_snapshot(last.index, last.term);
        // Installing a snapshot may discard conflicting entries beyond the
        // boundary; mark the log dirty from just past it so `take_persist`
        // reconciles the stored suffix (truncate + re-append the retained tail)
        // before the driver purges the compacted prefix (backlog A6).
        self.mark_log_dirty(LogIndex(last.index.0 + 1));
        self.snapshot = Some(StoredSnapshot {
            last_index: last.index,
            last_term: last.term,
            membership: is.last_config.clone(),
            data: is.data.clone(),
        });
        if self.commit_index < last.index {
            self.commit_index = last.index;
        }
        self.last_applied = last.index;
        self.outbox.push(Output::LoadSnapshot {
            index: last.index,
            data: is.data,
        });
        self.reply_snapshot(from);
    }

    fn reply_snapshot(&mut self, to: NodeId) {
        let reply = InstallSnapshotReply {
            term: self.current_term,
        };
        self.outbox
            .push(Output::Reply(to, RaftRpcReply::InstallSnapshot(reply)));
    }

    fn handle_snapshot_reply(&mut self, from: NodeId, reply: &InstallSnapshotReply) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        // The follower is now caught up to the snapshot boundary we sent.
        let upper = self
            .sent_upper
            .get(&from)
            .copied()
            .unwrap_or(LogIndex::ZERO);
        let current = self
            .match_index
            .get(&from)
            .copied()
            .unwrap_or(LogIndex::ZERO);
        if upper > current {
            self.match_index.insert(from, upper);
        }
        self.next_index.insert(from, upper.next());
        self.maybe_advance_commit();
    }
}
