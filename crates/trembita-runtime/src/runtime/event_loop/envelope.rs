use trembita_core::{ReadId, StateMachine};

use crate::DriverError;

use super::super::types::{ClientError, Envelope, NodeStatus};
use super::Runtime;

impl<M: StateMachine> Runtime<M> {
    #[allow(clippy::too_many_lines)]
    pub(in crate::runtime::event_loop) fn on_envelope(
        &mut self,
        env: Envelope<M>,
    ) -> Result<bool, DriverError> {
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
}
