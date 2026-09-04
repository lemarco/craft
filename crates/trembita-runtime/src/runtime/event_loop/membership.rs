use tokio::sync::oneshot;
use trembita_core::{
    MembershipError, StateMachine, occupied_node_ids, pick_promotion_candidate,
    plan_voter_replacement,
};
use trembita_proto::{
    JoinRejection, JoinRequest, JoinResponse, JoinRole, LeaveRejection, LeaveRequest,
    LeaveResponse, NodeId, PROTOCOL_VERSION, protocol_version_compatible,
};

use crate::DriverError;

use super::super::types::ClientError;
use super::Runtime;

impl<M: StateMachine> Runtime<M> {
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
    pub(in crate::runtime::event_loop) fn on_join(
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
    pub(in crate::runtime::event_loop) fn on_leave(
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
    pub(in crate::runtime::event_loop) fn maybe_replace_unreachable_voter(&mut self) {
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

    pub(in crate::runtime::event_loop) fn on_propose_membership(
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
