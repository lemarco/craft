use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use trembita_core::{
    CatalogProposeError, Command as _, MembershipError, ReadId, StateMachine, occupied_node_ids,
    pick_promotion_candidate, plan_catalog_expansion, plan_voter_replacement,
};
use trembita_net::{Transport, send_peer_rpc};
use trembita_proto::{
    CatalogAddRequest, CatalogAddResponse, CatalogCommand, CatalogRejection, JoinRejection,
    JoinRequest, JoinResponse, JoinRole, LeaveRejection, LeaveRequest, LeaveResponse, LogIndex,
    NodeId, PROTOCOL_VERSION, QueueAutoscalePolicyCommand, RaftRpc, RaftRpcReply,
    SagaJournalCommand, Term, TwoPhaseJournalCommand, protocol_version_compatible,
};

use crate::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};

use super::types::{
    CatalogAppliedFn, CatalogSnapshotFn, ClientError, Envelope, NodeStatus,
    QueueAutoscalePolicyAppliedFn, SagaJournalAppliedFn, TwoPhaseGcAbortedFn,
    TwoPhaseJournalAppliedFn,
};

type ReadConfirmSender = oneshot::Sender<Result<(LogIndex, Term), ClientError>>;
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

impl<M: StateMachine> Runtime<M> {
    #[allow(clippy::too_many_lines)] // constructor mirrors original spawn wiring.
    pub(super) fn new(
        driver: RaftDriver<M>,
        transport: Arc<dyn Transport>,
        config: &super::types::RuntimeConfig,
        self_tx: mpsc::UnboundedSender<Envelope<M>>,
        voter_replacement_grace_ticks: u64,
    ) -> Self {
        Self {
            driver,
            transport,
            self_tx,
            allow_join: config.allow_join,
            allow_voter_join: config.allow_voter_join,
            voter_replacement: config.voter_replacement,
            voter_replacement_grace_ticks,
            voter_unreachable_since: BTreeMap::new(),
            replacement_tick: 0,
            allow_leave: config.allow_leave,
            pending_proposals: HashMap::new(),
            pending_queries: HashMap::new(),
            pending_read_confirms: HashMap::new(),
            pending_joins: HashMap::new(),
            pending_leaves: HashMap::new(),
            pending_catalog_adds: HashMap::new(),
            pending_saga_journals: HashMap::new(),
            pending_two_phase_journals: HashMap::new(),
            pending_queue_autoscale_policies: HashMap::new(),
            pending_two_phase_prepares: HashMap::new(),
            pending_two_phase_aborts: HashMap::new(),
            pending_two_phase_commits: HashMap::new(),
            catalog_snapshot: config.catalog_snapshot.clone(),
            on_catalog_applied: config.on_catalog_applied.clone(),
            on_saga_journal_applied: config.on_saga_journal_applied.clone(),
            on_two_phase_journal_applied: config.on_two_phase_journal_applied.clone(),
            on_queue_autoscale_policy_applied: config.on_queue_autoscale_policy_applied.clone(),
            on_two_phase_gc_aborted: config.on_two_phase_gc_aborted.clone(),
            next_read_id: 0,
            cross_shard_2pc: config.cross_shard_2pc,
            durable_cross_shard_2pc: config.durable_cross_shard_2pc,
            two_phase_prepare_timeout: config.two_phase_prepare_timeout,
            tick_period: config.tick_period,
            two_phase_tick: 0,
            two_phase_prepares: crate::two_phase::PrepareStore::default(),
            compaction: config.compaction.clone(),
        }
    }

    pub(super) fn run_background(mut self, mut rx: mpsc::UnboundedReceiver<Envelope<M>>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.tick_period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut shutdown_done = None;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.two_phase_tick = self.two_phase_tick.saturating_add(1);
                        self.replacement_tick = self.replacement_tick.saturating_add(1);
                        match self.driver.tick() {
                            Ok(step) => { let _ = self.settle(step); }
                            Err(_) => break,
                        }
                        self.maybe_replace_unreachable_voter();
                        if self.maybe_gc_two_phase_prepares().is_err() {
                            break;
                        }
                    }
                    maybe = rx.recv() => {
                        let Some(env) = maybe else { break };
                        if let Envelope::Shutdown { done } = env {
                            shutdown_done = done;
                            break;
                        }
                        match self.on_envelope(env) {
                            Ok(true) => {}
                            Ok(false) | Err(_) => break,
                        }
                    }
                }
            }
            drop(self);
            if let Some(done) = shutdown_done {
                let _ = done.send(());
            }
            // Pending responders drop here, so blocked clients observe `Stopped`.
        });
    }

    /// Dispatch one outbound request RPC; feed its reply back into the mailbox.
    fn dispatch_send(&self, peer: NodeId, rpc: RaftRpc) {
        let transport = Arc::clone(&self.transport);
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            if let Ok(reply) = send_peer_rpc(&*transport, peer, &rpc).await {
                let _ = tx.send(Envelope::Reply { from: peer, reply });
            }
            // On transport error the peer is unreachable for now; the next
            // heartbeat/election round will retry. Nothing to feed back.
        });
    }

    /// Execute a step's effects and route applied/read results to waiting
    /// clients. Returns any reply effects (destined for a peer that made an
    /// inbound request) for the caller to hand back on that request.
    #[allow(clippy::too_many_lines)] // effect dispatch + client correlation in one pass.
    fn settle(&mut self, step: Step<M>) -> Vec<(NodeId, RaftRpcReply)> {
        let mut replies = Vec::new();
        for effect in step.effects {
            match effect {
                NetEffect::Send { peer, rpc } => self.dispatch_send(peer, rpc),
                NetEffect::Reply { peer, reply } => replies.push((peer, reply)),
            }
        }
        for (index, response) in step.applied {
            if let Some((tx_id, route_key, tx)) = self.pending_two_phase_commits.remove(&index) {
                self.two_phase_prepares.abort(&tx_id, &route_key);
                let _ = tx.send(Ok(response));
            } else if let Some(tx) = self.pending_proposals.remove(&index) {
                let _ = tx.send(Ok(response));
            }
        }
        for read in step.reads {
            match read {
                ReadOutcome::Ready { id, response } => {
                    if let Some(tx) = self.pending_queries.remove(&id) {
                        let _ = tx.send(Ok(response));
                    }
                }
                ReadOutcome::Confirmed { id, index } => {
                    if let Some(tx) = self.pending_read_confirms.remove(&id) {
                        let term = self.driver.node().current_term();
                        let _ = tx.send(Ok((index, term)));
                    }
                }
                ReadOutcome::Failed { id } => {
                    if let Some(tx) = self.pending_read_confirms.remove(&id) {
                        let _ = tx.send(Err(ClientError::NotLeader {
                            leader: self.driver.node().leader_id(),
                        }));
                    } else if let Some(tx) = self.pending_queries.remove(&id) {
                        let _ = tx.send(Err(ClientError::NotLeader {
                            leader: self.driver.node().leader_id(),
                        }));
                    }
                }
            }
        }
        for (index, command) in step.catalog_applied {
            if let Some(hook) = &self.on_catalog_applied {
                hook(command.clone());
            }
            if let Some(tx) = self.pending_catalog_adds.remove(&index) {
                let leader = self.driver.node().id();
                let CatalogCommand::AddGroups {
                    from_len,
                    new_groups,
                } = command;
                let _ = tx.send(CatalogAddResponse::Accepted {
                    leader,
                    catalog_len: from_len + u32::try_from(new_groups.len()).unwrap_or(u32::MAX),
                    new_groups,
                });
            }
        }
        for (index, command) in step.saga_journal_applied {
            if let Some(hook) = &self.on_saga_journal_applied {
                hook(command.clone());
            }
            if let Some(tx) = self.pending_saga_journals.remove(&index) {
                let _ = tx.send(Ok(()));
            }
        }
        for (index, command) in step.two_phase_journal_applied {
            if let Some(hook) = &self.on_two_phase_journal_applied {
                hook(command.clone());
            }
            if let Some(tx) = self.pending_two_phase_journals.remove(&index) {
                let _ = tx.send(Ok(()));
            }
        }
        for (index, command) in step.queue_autoscale_policy_applied {
            if let Some(hook) = &self.on_queue_autoscale_policy_applied {
                hook(command.clone());
            }
            if let Some(tx) = self.pending_queue_autoscale_policies.remove(&index) {
                let _ = tx.send(Ok(()));
            }
        }
        for (index, command) in step.two_phase_prepare_applied {
            if let Err(e) = self.two_phase_prepares.prepare(
                command.tx_id.clone(),
                command.route_key.clone(),
                command.command.clone(),
                self.two_phase_tick,
            ) {
                if let Some(tx) = self.pending_two_phase_prepares.remove(&index) {
                    let _ = tx.send(Err(ClientError::Driver(e.to_string())));
                }
                continue;
            }
            if let Some(tx) = self.pending_two_phase_prepares.remove(&index) {
                let _ = tx.send(Ok(()));
            }
        }
        for (index, command) in step.two_phase_abort_applied {
            let _ = self
                .two_phase_prepares
                .abort(&command.tx_id, &command.route_key);
            if let Some(tx) = self.pending_two_phase_aborts.remove(&index) {
                let _ = tx.send(Ok(()));
            }
        }
        // If we are no longer the leader, any still-outstanding client request
        // will never resolve here: an uncommitted proposal in our tail may be
        // overwritten by the new leader, and the core only reports read
        // failures. Fail them all with a `NotLeader` hint so callers stop
        // waiting and retry against the new leader. (A proposal that had
        // already committed applies above before this runs; anything failed
        // here may still commit under the new leader, so proposals must be
        // idempotent — see actor-state-redis.)
        self.resolve_committed_joins();
        self.resolve_committed_leaves();
        if !self.driver.is_leader() {
            self.fail_pending_requests();
        }
        self.maybe_auto_compact();
        replies
    }

    /// Snapshot and purge the log when the configured retention policy is met.
    fn maybe_auto_compact(&mut self) {
        if self.compaction.is_disabled() {
            return;
        }
        let stats = trembita_core::compaction_stats(self.driver.node());
        if !trembita_core::should_compact(&self.compaction, &stats) {
            return;
        }
        let _ = self.driver.compact();
    }

    /// Complete any join whose membership-change entry has now committed.
    fn resolve_committed_joins(&mut self) {
        if self.pending_joins.is_empty() {
            return;
        }
        let commit = self.driver.node().commit_index();
        let ready: Vec<LogIndex> = self
            .pending_joins
            .keys()
            .copied()
            .filter(|index| commit >= *index)
            .collect();
        if ready.is_empty() {
            return;
        }
        let leader = self.driver.node().id();
        let membership = self.driver.node().committed_membership();
        for index in ready {
            if let Some((tx, assigned)) = self.pending_joins.remove(&index) {
                let _ = tx.send(JoinResponse::Accepted {
                    leader,
                    node_id: assigned,
                    membership: membership.clone(),
                });
            }
        }
    }

    /// Complete any leave whose membership-change entry has now committed.
    fn resolve_committed_leaves(&mut self) {
        if self.pending_leaves.is_empty() {
            return;
        }
        let commit = self.driver.node().commit_index();
        let ready: Vec<LogIndex> = self
            .pending_leaves
            .keys()
            .copied()
            .filter(|index| commit >= *index)
            .collect();
        if ready.is_empty() {
            return;
        }
        let leader = self.driver.node().id();
        let membership = self.driver.node().committed_membership();
        for index in ready {
            if let Some(tx) = self.pending_leaves.remove(&index) {
                let _ = tx.send(LeaveResponse::Accepted {
                    leader,
                    membership: membership.clone(),
                });
            }
        }
    }

    /// Fail every outstanding client request and join with a leader hint after
    /// losing leadership.
    fn fail_pending_requests(&mut self) {
        let leader = self.driver.node().leader_id();
        for (_, tx) in self.pending_proposals.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_queries.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_read_confirms.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, (tx, _)) in self.pending_joins.drain() {
            let _ = tx.send(JoinResponse::Redirect { leader });
        }
        for (_, tx) in self.pending_leaves.drain() {
            let _ = tx.send(LeaveResponse::Redirect { leader });
        }
        for (_, tx) in self.pending_catalog_adds.drain() {
            let _ = tx.send(CatalogAddResponse::Redirect { leader });
        }
        for (_, tx) in self.pending_saga_journals.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_two_phase_journals.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_queue_autoscale_policies.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_two_phase_prepares.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, tx) in self.pending_two_phase_aborts.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        for (_, (_, _, tx)) in self.pending_two_phase_commits.drain() {
            let _ = tx.send(Err(ClientError::NotLeader { leader }));
        }
        if !self.durable_cross_shard_2pc {
            self.two_phase_prepares.clear();
        }
    }

    /// Process one mailbox message. Returns `Err` on a fatal driver failure
    /// (corrupt log / broken state machine), which stops the node.
    #[allow(clippy::too_many_lines)] // single envelope demux for the runtime loop.
    fn on_envelope(&mut self, env: Envelope<M>) -> Result<bool, DriverError> {
        match env {
            Envelope::Shutdown { .. } => return Ok(false),
            Envelope::Rpc { from, rpc, respond } => {
                let step = self.driver.deliver_rpc(from, rpc)?;
                let replies = self.settle(step);
                if let Some(reply) = replies
                    .into_iter()
                    .find_map(|(peer, reply)| (peer == from).then_some(reply))
                {
                    let _ = respond.send(reply);
                }
                // If no reply was produced the responder drops and the caller
                // observes a transport error — expected only for malformed input.
            }
            Envelope::Reply { from, reply } => {
                let step = self.driver.deliver_reply(from, reply)?;
                let _ = self.settle(step);
            }
            Envelope::Propose { command, respond } => match self.driver.propose(&command) {
                Ok((index, step)) => {
                    self.pending_proposals.insert(index, respond);
                    let _ = self.settle(step);
                }
                Err(DriverError::NotLeader { leader }) => {
                    let _ = respond.send(Err(ClientError::NotLeader { leader }));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            },
            Envelope::Query { query, respond } => {
                let id = ReadId(self.next_read_id);
                self.next_read_id += 1;
                match self.driver.query(id, query) {
                    Ok(step) => {
                        self.pending_queries.insert(id, respond);
                        let _ = self.settle(step);
                    }
                    Err(DriverError::NotLeader { leader }) => {
                        let _ = respond.send(Err(ClientError::NotLeader { leader }));
                    }
                    Err(e) => {
                        let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                    }
                }
            }
            Envelope::ConfirmReadIndex { respond } => {
                let id = ReadId(self.next_read_id);
                self.next_read_id += 1;
                match self.driver.confirm_read_index(id) {
                    Ok(step) => {
                        self.pending_read_confirms.insert(id, respond);
                        let _ = self.settle(step);
                    }
                    Err(DriverError::NotLeader { leader }) => {
                        let _ = respond.send(Err(ClientError::NotLeader { leader }));
                    }
                    Err(e) => {
                        let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                    }
                }
            }
            Envelope::LocalQuery { query, respond } => match self.driver.local_query(&query) {
                Ok(response) => {
                    let _ = respond.send(Ok(response));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            },
            Envelope::Join { request, respond } => {
                self.on_join(&request, respond)?;
            }
            Envelope::Leave { request, respond } => {
                self.on_leave(&request, respond)?;
            }
            Envelope::CatalogAdd { request, respond } => {
                self.on_catalog_add(&request, respond)?;
            }
            Envelope::UpsertSagaJournal { command, respond } => {
                self.on_upsert_saga_journal(command, respond)?;
            }
            Envelope::UpsertTwoPhaseJournal { command, respond } => {
                self.on_upsert_two_phase_journal(command, respond)?;
            }
            Envelope::UpsertQueueAutoscalePolicy { command, respond } => {
                self.on_upsert_queue_autoscale_policy(command, respond)?;
            }
            Envelope::ProposeMembership {
                voters,
                learners,
                respond,
            } => {
                self.on_propose_membership(voters, learners, respond)?;
            }
            Envelope::Campaign => {
                let step = self.driver.campaign()?;
                let _ = self.settle(step);
            }
            Envelope::Status { respond } => {
                let node = self.driver.node();
                let _ = respond.send(NodeStatus {
                    id: node.id(),
                    role: node.role(),
                    term: node.current_term(),
                    leader: node.leader_id(),
                    commit_index: node.commit_index(),
                    last_applied: node.last_applied(),
                    voters: node.voters(),
                    learners: node.committed_membership().learners,
                    reachable: node.reachable_now(),
                    reachable_members: node.reachable_members_now(),
                });
            }
            Envelope::ExportMigration { respond } => {
                let result = self
                    .driver
                    .export_migration()
                    .map_err(|e| ClientError::Driver(e.to_string()));
                let _ = respond.send(result);
            }
            Envelope::Compact { respond } => {
                let result = self
                    .driver
                    .compact()
                    .map_err(|e| ClientError::Driver(e.to_string()));
                let _ = respond.send(result);
            }
            Envelope::TwoPhasePrepare {
                tx_id,
                route_key,
                command,
                respond,
            } => {
                self.on_two_phase_prepare(tx_id, route_key, command, respond);
            }
            Envelope::TwoPhaseCommit {
                tx_id,
                route_key,
                respond,
            } => {
                self.on_two_phase_commit(tx_id, route_key, respond);
            }
            Envelope::TwoPhaseAbort {
                tx_id,
                route_key,
                respond,
            } => {
                self.on_two_phase_abort(tx_id, route_key, respond);
            }
        }
        Ok(true)
    }

    fn on_two_phase_prepare(
        &mut self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        command: Vec<u8>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) {
        if !self.cross_shard_2pc {
            let _ = respond.send(Err(ClientError::Driver(
                "cross-shard 2PC is disabled on this group".to_string(),
            )));
            return;
        }
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return;
        }
        if M::Command::from_bytes(&command).is_err() {
            let _ = respond.send(Err(ClientError::Driver(
                "decode command for 2PC prepare failed".to_string(),
            )));
            return;
        }
        if self.durable_cross_shard_2pc {
            let prepared_at_ms = crate::two_phase::unix_now_ms();
            let journal_cmd = trembita_proto::TwoPhasePrepareCommand {
                tx_id,
                route_key,
                command,
                prepared_at_ms,
            };
            match self.driver.propose_two_phase_prepare(journal_cmd) {
                Ok(Ok((index, step))) => {
                    self.pending_two_phase_prepares.insert(index, respond);
                    let _ = self.settle(step);
                }
                Ok(Err(trembita_core::CatalogProposeError::NotLeader { leader })) => {
                    let _ = respond.send(Err(ClientError::NotLeader { leader }));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            }
            return;
        }
        match self
            .two_phase_prepares
            .prepare(tx_id, route_key, command, self.two_phase_tick)
        {
            Ok(()) => {
                let _ = respond.send(Ok(()));
            }
            Err(e) => {
                let _ = respond.send(Err(ClientError::Driver(e.to_string())));
            }
        }
    }

    fn on_two_phase_commit(
        &mut self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    ) {
        if !self.cross_shard_2pc {
            let _ = respond.send(Err(ClientError::Driver(
                "cross-shard 2PC is disabled on this group".to_string(),
            )));
            return;
        }
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return;
        }
        let Some(bytes) = self.two_phase_prepares.get(&tx_id, &route_key).cloned() else {
            let _ = respond.send(Err(ClientError::Driver(
                "no prepared command for transaction key".to_string(),
            )));
            return;
        };
        let command = match M::Command::from_bytes(&bytes) {
            Ok(c) => c,
            Err(e) => {
                let _ = respond.send(Err(ClientError::Driver(format!(
                    "decode prepared command: {e}"
                ))));
                return;
            }
        };
        match self.driver.propose(&command) {
            Ok((index, step)) => {
                self.pending_two_phase_commits
                    .insert(index, (tx_id, route_key, respond));
                let _ = self.settle(step);
            }
            Err(DriverError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
            Err(e) => {
                let _ = respond.send(Err(ClientError::Driver(e.to_string())));
            }
        }
    }

    fn on_two_phase_abort(
        &mut self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) {
        if !self.cross_shard_2pc {
            let _ = respond.send(Err(ClientError::Driver(
                "cross-shard 2PC is disabled on this group".to_string(),
            )));
            return;
        }
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return;
        }
        if self.durable_cross_shard_2pc {
            let journal_cmd = trembita_proto::TwoPhaseAbortCommand { tx_id, route_key };
            match self.driver.propose_two_phase_abort(journal_cmd) {
                Ok(Ok((index, step))) => {
                    self.pending_two_phase_aborts.insert(index, respond);
                    let _ = self.settle(step);
                }
                Ok(Err(trembita_core::CatalogProposeError::NotLeader { leader })) => {
                    let _ = respond.send(Err(ClientError::NotLeader { leader }));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            }
            return;
        }
        let _ = self.two_phase_prepares.abort(&tx_id, &route_key);
        let _ = respond.send(Ok(()));
    }

    /// Abort prepares that exceeded [`two_phase_prepare_timeout`] (leader-only).
    fn maybe_gc_two_phase_prepares(&mut self) -> Result<(), DriverError> {
        let Some(timeout) = self.two_phase_prepare_timeout else {
            return Ok(());
        };
        if !self.cross_shard_2pc || !self.driver.is_leader() {
            return Ok(());
        }
        let tick_period_ms = u64::try_from(
            self.tick_period
                .as_millis()
                .max(1)
                .min(u128::from(u64::MAX)),
        )
        .unwrap_or(u64::MAX);
        let timeout_ms =
            u64::try_from(timeout.as_millis().max(1).min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
        let timeout_ticks = timeout_ms.div_ceil(tick_period_ms).max(1);
        let expired = self
            .two_phase_prepares
            .expired_ticks(self.two_phase_tick, timeout_ticks);
        for (tx_id, route_key) in expired {
            if self.durable_cross_shard_2pc {
                let journal_cmd = trembita_proto::TwoPhaseAbortCommand { tx_id, route_key };
                match self.driver.propose_two_phase_abort(journal_cmd)? {
                    Ok((_, step)) => {
                        let _ = self.settle(step);
                        if let Some(hook) = &self.on_two_phase_gc_aborted {
                            hook();
                        }
                    }
                    Err(trembita_core::CatalogProposeError::NotLeader { .. }) => break,
                }
            } else {
                let _ = self.two_phase_prepares.abort(&tx_id, &route_key);
                if let Some(hook) = &self.on_two_phase_gc_aborted {
                    hook();
                }
            }
        }
        Ok(())
    }

    fn next_free_node_id(voters: &[NodeId], learners: &[NodeId]) -> NodeId {
        let occupied = occupied_node_ids(voters, learners);
        let mut n = 1u64;
        loop {
            let candidate = NodeId(n);
            if !occupied.contains(&candidate) {
                return candidate;
            }
            n += 1;
        }
    }

    /// Validate and (on the leader) start a cluster join as a membership change
    /// (join-rpc, join-version-skew). The join resolves to [`JoinResponse::Accepted`] once the
    /// membership entry commits (see [`resolve_committed_joins`]).
    fn on_join(
        &mut self,
        request: &JoinRequest,
        respond: oneshot::Sender<JoinResponse>,
    ) -> Result<(), DriverError> {
        // Hard-reject a protocol-version mismatch before anything else (join-version-skew).
        if !protocol_version_compatible(request.protocol_version) {
            let _ = respond.send(JoinResponse::Rejected {
                reason: JoinRejection::VersionSkew {
                    expected: PROTOCOL_VERSION,
                    got: request.protocol_version,
                },
            });
            return Ok(());
        }
        if !self.allow_join {
            let _ = respond.send(JoinResponse::Rejected {
                reason: JoinRejection::JoinsDisabled,
            });
            return Ok(());
        }
        if !self.driver.is_leader() {
            let _ = respond.send(JoinResponse::Redirect {
                leader: self.driver.node().leader_id(),
            });
            return Ok(());
        }
        let membership = self.driver.node().committed_membership();
        let mut voters = membership.voters;
        let mut learners = membership.learners;
        let assigned = match request.node_id {
            Some(id) => id,
            None => Self::next_free_node_id(&voters, &learners),
        };
        if voters.contains(&assigned) || learners.contains(&assigned) {
            let _ = respond.send(JoinResponse::Rejected {
                reason: JoinRejection::Duplicate,
            });
            return Ok(());
        }
        match request.role {
            JoinRole::Learner => learners.push(assigned),
            JoinRole::Voter => {
                if !self.allow_voter_join {
                    let _ = respond.send(JoinResponse::Rejected {
                        reason: JoinRejection::VoterJoinDisabled,
                    });
                    return Ok(());
                }
                voters.push(assigned);
            }
        }
        learners.sort();
        learners.dedup();

        match self.driver.propose_membership(voters, learners)? {
            Ok((index, step)) => {
                self.pending_joins.insert(index, (respond, assigned));
                let _ = self.settle(step);
            }
            Err(MembershipError::NotLeader { leader }) => {
                let _ = respond.send(JoinResponse::Redirect { leader });
            }
            Err(MembershipError::InProgress) => {
                let _ = respond.send(JoinResponse::Rejected {
                    reason: JoinRejection::Other(
                        "a membership change is already in progress".to_string(),
                    ),
                });
            }
            Err(MembershipError::EmptyVoters) => {
                let _ = respond.send(JoinResponse::Rejected {
                    reason: JoinRejection::Other("resulting voter set is empty".to_string()),
                });
            }
        }
        Ok(())
    }

    /// Validate and (on the leader) start a cluster leave as a membership change.
    /// The leave resolves to [`LeaveResponse::Accepted`] once the membership
    /// entry commits (see [`resolve_committed_leaves`]).
    fn on_leave(
        &mut self,
        request: &LeaveRequest,
        respond: oneshot::Sender<LeaveResponse>,
    ) -> Result<(), DriverError> {
        if !protocol_version_compatible(request.protocol_version) {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::VersionSkew {
                    expected: PROTOCOL_VERSION,
                    got: request.protocol_version,
                },
            });
            return Ok(());
        }
        if !self.allow_leave {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::LeavesDisabled,
            });
            return Ok(());
        }
        if !self.driver.is_leader() {
            let _ = respond.send(LeaveResponse::Redirect {
                leader: self.driver.node().leader_id(),
            });
            return Ok(());
        }
        let membership = self.driver.node().committed_membership();
        let mut voters = membership.voters;
        let mut learners = membership.learners;
        if voters.contains(&request.node_id) {
            if voters.len() <= 1 {
                let _ = respond.send(LeaveResponse::Rejected {
                    reason: LeaveRejection::LastMember,
                });
                return Ok(());
            }
            voters.retain(|id| *id != request.node_id);
        } else if learners.contains(&request.node_id) {
            learners.retain(|id| *id != request.node_id);
        } else {
            let _ = respond.send(LeaveResponse::Rejected {
                reason: LeaveRejection::NotMember,
            });
            return Ok(());
        }

        match self.driver.propose_membership(voters, learners)? {
            Ok((index, step)) => {
                self.pending_leaves.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(MembershipError::NotLeader { leader }) => {
                let _ = respond.send(LeaveResponse::Redirect { leader });
            }
            Err(MembershipError::InProgress) => {
                let _ = respond.send(LeaveResponse::Rejected {
                    reason: LeaveRejection::Other(
                        "a membership change is already in progress".to_string(),
                    ),
                });
            }
            Err(MembershipError::EmptyVoters) => {
                let _ = respond.send(LeaveResponse::Rejected {
                    reason: LeaveRejection::LastMember,
                });
            }
        }
        Ok(())
    }

    /// Validate and (on the group 0 leader) replicate a catalog expansion.
    fn on_catalog_add(
        &mut self,
        request: &CatalogAddRequest,
        respond: oneshot::Sender<CatalogAddResponse>,
    ) -> Result<(), DriverError> {
        if !protocol_version_compatible(request.protocol_version) {
            let _ = respond.send(CatalogAddResponse::Rejected {
                reason: CatalogRejection::VersionSkew {
                    expected: PROTOCOL_VERSION,
                    got: request.protocol_version,
                },
            });
            return Ok(());
        }
        if !self.driver.is_leader() {
            let _ = respond.send(CatalogAddResponse::Redirect {
                leader: self.driver.node().leader_id(),
            });
            return Ok(());
        }
        let Some(snapshot) = &self.catalog_snapshot else {
            let _ = respond.send(CatalogAddResponse::Rejected {
                reason: CatalogRejection::NotMultiRaft,
            });
            return Ok(());
        };
        let catalog = snapshot();
        let plan = match plan_catalog_expansion(&catalog, request.add_groups) {
            Ok(plan) => plan,
            Err(e) => {
                let _ = respond.send(CatalogAddResponse::Rejected {
                    reason: CatalogRejection::InvalidExpansion(e.to_string()),
                });
                return Ok(());
            }
        };
        let command = CatalogCommand::AddGroups {
            from_len: plan.from_len,
            new_groups: plan.new_groups.iter().map(|g| g.0).collect(),
        };
        match self.driver.propose_catalog(command)? {
            Ok((index, step)) => {
                self.pending_catalog_adds.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(CatalogProposeError::NotLeader { leader }) => {
                let _ = respond.send(CatalogAddResponse::Redirect { leader });
            }
        }
        Ok(())
    }

    /// Replicate a saga journal upsert on the group 0 leader.
    fn on_upsert_saga_journal(
        &mut self,
        command: SagaJournalCommand,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) -> Result<(), DriverError> {
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return Ok(());
        }
        match self.driver.propose_saga_journal(command)? {
            Ok((index, step)) => {
                self.pending_saga_journals.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(CatalogProposeError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
        }
        Ok(())
    }

    /// Replicate a 2PC client journal upsert on the Meta-Raft / group 0 leader.
    fn on_upsert_two_phase_journal(
        &mut self,
        command: TwoPhaseJournalCommand,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) -> Result<(), DriverError> {
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return Ok(());
        }
        match self.driver.propose_two_phase_journal(command)? {
            Ok((index, step)) => {
                self.pending_two_phase_journals.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(CatalogProposeError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
        }
        Ok(())
    }

    /// Replicate a queue autoscale policy upsert on the Meta-Raft / group 0 leader.
    fn on_upsert_queue_autoscale_policy(
        &mut self,
        command: QueueAutoscalePolicyCommand,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) -> Result<(), DriverError> {
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return Ok(());
        }
        match self.driver.propose_queue_autoscale_policy(command)? {
            Ok((index, step)) => {
                self.pending_queue_autoscale_policies.insert(index, respond);
                let _ = self.settle(step);
            }
            Err(CatalogProposeError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
        }
        Ok(())
    }

    /// Leader-only: when a voter stays unreachable beyond the grace window,
    /// remove it and promote the lowest-id caught-up learner (voter elasticity).
    fn maybe_replace_unreachable_voter(&mut self) {
        if !self.voter_replacement || !self.driver.is_leader() {
            return;
        }
        let node = self.driver.node();
        if node.is_joint() || !self.pending_joins.is_empty() || !self.pending_leaves.is_empty() {
            return;
        }
        let membership = node.committed_membership();
        if membership.learners.is_empty() {
            return;
        }
        let reachable = node.reachable_now();
        let now = self.replacement_tick;
        let grace = self.voter_replacement_grace_ticks;
        let match_index = node.peer_match_indices();
        let commit_index = node.commit_index();
        let self_id = node.id();

        let mut to_replace: Option<NodeId> = None;
        for voter in &membership.voters {
            if *voter == self_id || reachable.contains(voter) {
                self.voter_unreachable_since.remove(voter);
                continue;
            }
            let since = self.voter_unreachable_since.entry(*voter).or_insert(now);
            if now.saturating_sub(*since) >= grace {
                to_replace = Some(*voter);
                break;
            }
        }
        let Some(dead) = to_replace else {
            return;
        };
        let Some(promote) =
            pick_promotion_candidate(&membership.learners, &match_index, commit_index)
        else {
            return;
        };
        let (voters, learners) =
            plan_voter_replacement(dead, membership.voters, membership.learners, promote);
        self.voter_unreachable_since.remove(&dead);
        if let Ok(Ok((_, step))) = self.driver.propose_membership(voters, learners) {
            let _ = self.settle(step);
        }
    }

    fn on_propose_membership(
        &mut self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) -> Result<(), DriverError> {
        use trembita_core::MembershipError;

        match self.driver.propose_membership(voters, learners)? {
            Ok((_, step)) => {
                let _ = self.settle(step);
                let _ = respond.send(Ok(()));
            }
            Err(MembershipError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
            Err(MembershipError::InProgress) => {
                let _ = respond.send(Err(ClientError::Driver(
                    "a membership change is already in progress".to_string(),
                )));
            }
            Err(MembershipError::EmptyVoters) => {
                let _ = respond.send(Err(ClientError::Driver(
                    "resulting voter set is empty".to_string(),
                )));
            }
        }
        Ok(())
    }
}
