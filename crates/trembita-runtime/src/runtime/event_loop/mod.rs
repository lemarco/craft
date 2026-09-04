use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use trembita_core::{ReadId, StateMachine};
use trembita_net::Transport;
use trembita_proto::{CatalogAddResponse, JoinResponse, LeaveResponse, LogIndex, NodeId};

use crate::RaftDriver;

use super::types::{
    CatalogAppliedFn, CatalogSnapshotFn, ClientError, Envelope, QueueAutoscalePolicyAppliedFn,
    SagaJournalAppliedFn, TwoPhaseGcAbortedFn, TwoPhaseJournalAppliedFn,
};

mod catalog;
mod core;
mod envelope;
mod membership;
mod settle;
mod two_phase;

type ReadConfirmSender = oneshot::Sender<Result<(LogIndex, trembita_proto::Term), ClientError>>;
type PendingTwoPhaseCommit<M> = (
    Vec<u8>,
    Vec<u8>,
    oneshot::Sender<Result<<M as StateMachine>::Response, ClientError>>,
);

/// Owns the driver and mutable correlation state inside the loop task.
#[allow(clippy::struct_excessive_bools)] // runtime flags + correlation maps share one loop state.
pub(super) struct Runtime<M: StateMachine> {
    driver: RaftDriver<M>,
    transport: Arc<dyn Transport>,
    self_tx: mpsc::UnboundedSender<Envelope<M>>,
    allow_join: bool,
    allow_voter_join: bool,
    voter_replacement: bool,
    voter_replacement_grace_ticks: u64,
    voter_unreachable_since: BTreeMap<NodeId, u64>,
    replacement_tick: u64,
    allow_leave: bool,
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<M::Response, ClientError>>>,
    pending_queries: HashMap<ReadId, oneshot::Sender<Result<M::Response, ClientError>>>,
    /// Leader `ReadIndex` confirmations awaiting quorum ack (follower-read setup).
    pending_read_confirms: HashMap<ReadId, ReadConfirmSender>,
    /// Join requests awaiting their membership-change entry to commit, keyed by
    /// that entry's log index.
    pending_joins: HashMap<LogIndex, (oneshot::Sender<JoinResponse>, NodeId)>,
    /// Leave requests awaiting their membership-change entry to commit.
    pending_leaves: HashMap<LogIndex, oneshot::Sender<LeaveResponse>>,
    /// Catalog add requests awaiting their catalog entry to commit.
    pending_catalog_adds: HashMap<LogIndex, oneshot::Sender<CatalogAddResponse>>,
    pending_saga_journals: HashMap<LogIndex, oneshot::Sender<Result<(), ClientError>>>,
    pending_two_phase_journals: HashMap<LogIndex, oneshot::Sender<Result<(), ClientError>>>,
    pending_queue_autoscale_policies: HashMap<LogIndex, oneshot::Sender<Result<(), ClientError>>>,
    pending_two_phase_prepares: HashMap<LogIndex, oneshot::Sender<Result<(), ClientError>>>,
    pending_two_phase_aborts: HashMap<LogIndex, oneshot::Sender<Result<(), ClientError>>>,
    pending_two_phase_commits: HashMap<LogIndex, PendingTwoPhaseCommit<M>>,
    catalog_snapshot: Option<CatalogSnapshotFn>,
    on_catalog_applied: Option<CatalogAppliedFn>,
    on_saga_journal_applied: Option<SagaJournalAppliedFn>,
    on_two_phase_journal_applied: Option<TwoPhaseJournalAppliedFn>,
    on_queue_autoscale_policy_applied: Option<QueueAutoscalePolicyAppliedFn>,
    on_two_phase_gc_aborted: Option<TwoPhaseGcAbortedFn>,
    next_read_id: u64,
    cross_shard_2pc: bool,
    durable_cross_shard_2pc: bool,
    two_phase_prepare_timeout: Option<Duration>,
    tick_period: Duration,
    two_phase_tick: u64,
    two_phase_prepares: crate::two_phase::PrepareStore,
    compaction: trembita_core::CompactionPolicy,
}
