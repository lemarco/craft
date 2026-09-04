use trembita_proto::{
    CatalogCommand, LogEntry, LogId, LogIndex, Membership, NodeId, QueueAutoscalePolicyCommand,
    RaftRpc, RaftRpcReply, SagaJournalCommand, Term, TwoPhaseAbortCommand, TwoPhaseJournalCommand,
    TwoPhasePrepareCommand,
};

use crate::failure_detector::ReachabilityConfig;

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

/// Returned by [`super::RaftNode::propose`] / [`super::RaftNode::read_index`] when the node
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
/// [`super::RaftNode::take_persist`]; it is the delta since the previous call.
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
/// application bytes. Returned by [`super::RaftNode::stored_snapshot`] so a runtime can
/// persist the snapshot durably and purge the compacted log prefix (backlog
/// A6), and fed back to [`super::RaftNode::restore_with_snapshot`] on restart.
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
