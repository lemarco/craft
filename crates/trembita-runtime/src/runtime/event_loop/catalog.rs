use tokio::sync::oneshot;
use trembita_core::{CatalogProposeError, StateMachine, plan_catalog_expansion};
use trembita_proto::{
    CatalogAddRequest, CatalogAddResponse, CatalogCommand, CatalogRejection, NodeId,
    PROTOCOL_VERSION, QueueAutoscalePolicyCommand, SagaJournalCommand, TwoPhaseJournalCommand,
    protocol_version_compatible,
};

use crate::DriverError;

use super::super::types::ClientError;
use super::Runtime;

impl<M: StateMachine> Runtime<M> {
    pub(in crate::runtime::event_loop) fn on_catalog_add(
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
    pub(in crate::runtime::event_loop) fn on_upsert_saga_journal(
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
    pub(in crate::runtime::event_loop) fn on_upsert_two_phase_journal(
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
    pub(in crate::runtime::event_loop) fn on_upsert_queue_autoscale_policy(
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
}
