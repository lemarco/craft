use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use trembita_core::{Role, StateMachine};
use trembita_proto::{
    CatalogAddRequest, CatalogAddResponse, CatalogCommand, JoinRequest, JoinResponse, LeaveRequest,
    LeaveResponse, LogIndex, NodeId, QueueAutoscalePolicyCommand, RaftRpc, RaftRpcReply,
    SagaJournalCommand, Term, TwoPhaseJournalCommand,
};

/// An error returned to a client whose request could not be completed.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// This node is not the leader; retry against `leader` (transparent
    /// forwarding is a later increment, client-routing).
    #[error("not leader (leader hint: {leader:?})")]
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// The node runtime has stopped (shut down or crashed); the result will
    /// never arrive.
    #[error("node runtime stopped")]
    Stopped,
    /// A driver-level failure while servicing the request.
    #[error("{0}")]
    Driver(String),
}

/// A point-in-time snapshot of a node's consensus status (observability).
#[derive(Debug, Clone)]
pub struct NodeStatus {
    /// This node's id.
    pub id: NodeId,
    /// Current role.
    pub role: Role,
    /// Current term.
    pub term: Term,
    /// Best-known leader, if any.
    pub leader: Option<NodeId>,
    /// Highest committed index.
    pub commit_index: LogIndex,
    /// Highest applied index.
    pub last_applied: LogIndex,
    /// The current committed voter set (Raft membership), sorted.
    pub voters: Vec<NodeId>,
    /// Non-voting learners in the committed configuration, sorted.
    pub learners: Vec<NodeId>,
    /// The voters this node currently considers **reachable** — a liveness
    /// signal distinct from committed membership (liveness-vs-membership). On the leader this
    /// drops voters that have stopped acking heartbeats (crashed / partitioned)
    /// even though they remain committed voters; a follower reports all voters.
    pub reachable: Vec<NodeId>,
    /// Voters plus learners believed reachable (worker placement / auto-spawn).
    pub reachable_members: Vec<NodeId>,
}

/// Internal mailbox messages processed by the runtime loop.
pub(super) enum Envelope<M: StateMachine> {
    Rpc {
        from: NodeId,
        rpc: RaftRpc,
        respond: oneshot::Sender<RaftRpcReply>,
    },
    Reply {
        from: NodeId,
        reply: RaftRpcReply,
    },
    Propose {
        command: M::Command,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    Query {
        query: M::Query,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    /// Leader-only: confirm a `ReadIndex` without executing a query.
    ConfirmReadIndex {
        respond: oneshot::Sender<Result<(LogIndex, Term), ClientError>>,
    },
    /// Follower-only: query local state after the apply barrier.
    LocalQuery {
        query: M::Query,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    Join {
        request: JoinRequest,
        respond: oneshot::Sender<JoinResponse>,
    },
    Leave {
        request: LeaveRequest,
        respond: oneshot::Sender<LeaveResponse>,
    },
    CatalogAdd {
        request: CatalogAddRequest,
        respond: oneshot::Sender<CatalogAddResponse>,
    },
    UpsertSagaJournal {
        command: SagaJournalCommand,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    UpsertTwoPhaseJournal {
        command: TwoPhaseJournalCommand,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    UpsertQueueAutoscalePolicy {
        command: QueueAutoscalePolicyCommand,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    /// Leader-only: begin a joint-consensus membership change (per-group-raft-membership).
    ProposeMembership {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    Campaign,
    Status {
        respond: oneshot::Sender<NodeStatus>,
    },
    ExportMigration {
        respond: oneshot::Sender<Result<trembita_proto::GroupMigrationBundle, ClientError>>,
    },
    /// Compact the applied log prefix into a durable snapshot (Raft §7).
    Compact {
        respond: oneshot::Sender<Result<bool, ClientError>>,
    },
    TwoPhasePrepare {
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        command: Vec<u8>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    TwoPhaseCommit {
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    },
    TwoPhaseAbort {
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    Shutdown {
        done: Option<oneshot::Sender<()>>,
    },
}

/// Snapshot of the live multi-Raft catalog for group 0 expansion planning.
pub type CatalogSnapshotFn = Arc<dyn Fn() -> Vec<trembita_core::RaftGroupId> + Send + Sync>;
/// Hook invoked when a catalog entry commits on this node (all group 0 replicas).
pub type CatalogAppliedFn = Arc<dyn Fn(CatalogCommand) + Send + Sync>;
/// Hook invoked when a saga journal entry commits on this node (group 0 replicas).
pub type SagaJournalAppliedFn = Arc<dyn Fn(SagaJournalCommand) + Send + Sync>;
/// Hook invoked when a 2PC client journal entry commits on this node (Meta-Raft replicas).
pub type TwoPhaseJournalAppliedFn = Arc<dyn Fn(TwoPhaseJournalCommand) + Send + Sync>;
/// Hook invoked when a queue autoscale policy entry commits (Meta-Raft / group 0 replicas).
pub type QueueAutoscalePolicyAppliedFn = Arc<dyn Fn(QueueAutoscalePolicyCommand) + Send + Sync>;
/// Hook invoked when the leader GC-aborts a stale durable 2PC prepare.
pub type TwoPhaseGcAbortedFn = Arc<dyn Fn() + Send + Sync>;

/// Tunables for the runtime loop.
#[allow(clippy::struct_excessive_bools)] // feature flags + hook toggles in one config struct.
#[derive(Clone)]
pub struct RuntimeConfig {
    /// Wall-clock duration of one logical Raft tick. The core's timeouts are in
    /// ticks (see [`trembita_core::Config`]); this maps them onto real time.
    pub tick_period: Duration,
    /// Whether this node accepts cluster joins (`--allow-join`, join-rpc). When
    /// `false`, `/cluster/join` requests are rejected with
    /// [`trembita_proto::JoinRejection::JoinsDisabled`].
    pub allow_join: bool,
    /// Whether join requests with [`trembita_proto::JoinRole::Voter`] are accepted. Elastic
    /// scale-out uses [`trembita_proto::JoinRole::Learner`] (default); voter joins are for rare
    /// control-plane expansion only.
    pub allow_voter_join: bool,
    /// When `true`, the leader replaces a voter unreachable beyond the grace
    /// window by promoting the lowest-id caught-up learner.
    pub voter_replacement: bool,
    /// Override grace window in logical ticks before voter replacement. `None`
    /// uses `6 ×` the reachability silence window.
    pub voter_replacement_grace_ticks: Option<u64>,
    /// Whether this node accepts cluster leaves (`--allow-leave`). When `false`,
    /// `/cluster/leave` requests are rejected with
    /// [`trembita_proto::LeaveRejection::LeavesDisabled`].
    pub allow_leave: bool,
    /// Live catalog for [`CatalogAddRequest`] planning (group 0 multi-Raft only).
    pub catalog_snapshot: Option<CatalogSnapshotFn>,
    /// Apply hook for committed catalog entries (group 0 multi-Raft only).
    pub on_catalog_applied: Option<CatalogAppliedFn>,
    /// Apply hook for committed saga journal entries (group 0 only).
    pub on_saga_journal_applied: Option<SagaJournalAppliedFn>,
    /// Apply hook for committed 2PC client journal entries (Meta-Raft / group 0).
    pub on_two_phase_journal_applied: Option<TwoPhaseJournalAppliedFn>,
    /// Apply hook for committed queue autoscale policy entries (Meta-Raft / group 0).
    pub on_queue_autoscale_policy_applied: Option<QueueAutoscalePolicyAppliedFn>,
    /// Metrics hook when a stale durable prepare is GC-aborted (leader-only).
    pub on_two_phase_gc_aborted: Option<TwoPhaseGcAbortedFn>,
    /// Enable cross-shard two-phase commit prepare/commit/abort on this group.
    pub cross_shard_2pc: bool,
    /// Persist 2PC prepare/abort in the Raft log (requires `cross_shard_2pc`).
    pub durable_cross_shard_2pc: bool,
    /// Drop staged prepares older than this (leader-only). `None` disables GC.
    pub two_phase_prepare_timeout: Option<Duration>,
    /// Automatic log compaction policy (Raft §7). Disabled when both thresholds
    /// are unset; see [`trembita_core::CompactionPolicy`].
    pub compaction: trembita_core::CompactionPolicy,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            tick_period: Duration::from_millis(50),
            allow_join: false,
            allow_voter_join: false,
            voter_replacement: true,
            voter_replacement_grace_ticks: None,
            allow_leave: false,
            catalog_snapshot: None,
            on_catalog_applied: None,
            on_saga_journal_applied: None,
            on_two_phase_journal_applied: None,
            on_queue_autoscale_policy_applied: None,
            on_two_phase_gc_aborted: None,
            cross_shard_2pc: false,
            durable_cross_shard_2pc: false,
            two_phase_prepare_timeout: None,
            compaction: trembita_core::CompactionPolicy::default(),
        }
    }
}

impl std::fmt::Debug for RuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("tick_period", &self.tick_period)
            .field("allow_join", &self.allow_join)
            .field("allow_voter_join", &self.allow_voter_join)
            .field("voter_replacement", &self.voter_replacement)
            .field(
                "voter_replacement_grace_ticks",
                &self.voter_replacement_grace_ticks,
            )
            .field("allow_leave", &self.allow_leave)
            .field(
                "catalog_snapshot",
                &self.catalog_snapshot.as_ref().map(|_| "<fn>"),
            )
            .field(
                "on_catalog_applied",
                &self.on_catalog_applied.as_ref().map(|_| "<fn>"),
            )
            .field(
                "on_saga_journal_applied",
                &self.on_saga_journal_applied.as_ref().map(|_| "<fn>"),
            )
            .field(
                "on_two_phase_journal_applied",
                &self.on_two_phase_journal_applied.as_ref().map(|_| "<fn>"),
            )
            .field(
                "on_queue_autoscale_policy_applied",
                &self
                    .on_queue_autoscale_policy_applied
                    .as_ref()
                    .map(|_| "<fn>"),
            )
            .field(
                "on_two_phase_gc_aborted",
                &self.on_two_phase_gc_aborted.as_ref().map(|_| "<fn>"),
            )
            .field("cross_shard_2pc", &self.cross_shard_2pc)
            .field("durable_cross_shard_2pc", &self.durable_cross_shard_2pc)
            .field("two_phase_prepare_timeout", &self.two_phase_prepare_timeout)
            .field("compaction", &self.compaction)
            .finish()
    }
}
