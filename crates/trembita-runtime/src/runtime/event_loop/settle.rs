use trembita_core::StateMachine;
use trembita_proto::{
    CatalogAddResponse, CatalogCommand, JoinResponse, LeaveResponse, LogIndex, NodeId, RaftRpcReply,
};

use crate::{NetEffect, ReadOutcome, Step};

use super::super::types::ClientError;
use super::Runtime;

impl<M: StateMachine> Runtime<M> {
    #[allow(clippy::too_many_lines)]
    pub(in crate::runtime::event_loop) fn settle(
        &mut self,
        step: Step<M>,
    ) -> Vec<(NodeId, RaftRpcReply)> {
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
}
