//! Per-group Raft membership sync (per-group-raft-membership Phase 2).

use std::collections::BTreeMap;

use trembita_core::{GroupReplicationTarget, RaftGroupId, Role, plan_group_membership_sync};
use trembita_proto::NodeId;

use crate::NodeHandle;
use crate::runtime::ClientError;
use trembita_core::StateMachine;

/// Outcome of a membership sync pass over locally hosted groups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupMembershipSyncReport {
    /// Groups where a membership change was proposed on this node.
    pub proposed: Vec<RaftGroupId>,
    /// Groups skipped because this node is not that group's Raft leader.
    pub skipped_not_leader: Vec<RaftGroupId>,
    /// Groups skipped because a change is already in flight.
    pub skipped_in_progress: Vec<RaftGroupId>,
}

/// Collect committed voter and learner sets for hosted groups from live handles.
pub async fn collect_group_membership<M: StateMachine>(
    hosted: &[(u32, NodeHandle<M>)],
) -> (
    BTreeMap<RaftGroupId, Vec<NodeId>>,
    BTreeMap<RaftGroupId, Vec<NodeId>>,
) {
    let mut voters = BTreeMap::new();
    let mut learners = BTreeMap::new();
    for (id, handle) in hosted {
        if let Some(status) = handle.status().await {
            voters.insert(RaftGroupId(*id), status.voters);
            learners.insert(RaftGroupId(*id), status.learners);
        }
    }
    (voters, learners)
}

/// Apply one membership sync pass: for each hosted group where this node is
/// leader, propose the desired voter + learner sets from the pure planner.
pub async fn sync_hosted_group_membership<M: StateMachine>(
    hosted: &[(u32, NodeHandle<M>)],
    live_nodes: &[NodeId],
    catalog: &[RaftGroupId],
    replication_factor: u32,
    learner_factor: u32,
) -> GroupMembershipSyncReport {
    let (current_voters, current_learners) = collect_group_membership(hosted).await;
    let desired = plan_group_membership_sync(
        catalog,
        live_nodes,
        &current_voters,
        &current_learners,
        replication_factor,
        learner_factor,
    );
    let mut report = GroupMembershipSyncReport::default();

    for (id, handle) in hosted {
        let group = RaftGroupId(*id);
        let Some(GroupReplicationTarget {
            voters: target_voters,
            learners: target_learners,
        }) = desired.get(&group)
        else {
            continue;
        };
        let Some(status) = handle.status().await else {
            continue;
        };
        if status.role != Role::Leader {
            report.skipped_not_leader.push(group);
            continue;
        }
        match handle
            .propose_membership(target_voters.clone(), target_learners.clone())
            .await
        {
            Ok(()) => report.proposed.push(group),
            Err(ClientError::Driver(msg)) if msg.contains("in progress") => {
                report.skipped_in_progress.push(group);
            }
            Err(ClientError::NotLeader { .. }) => {
                report.skipped_not_leader.push(group);
            }
            Err(_) => {}
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_report_tracks_proposed_groups() {
        let report = GroupMembershipSyncReport {
            proposed: vec![RaftGroupId(1), RaftGroupId(2)],
            ..GroupMembershipSyncReport::default()
        };
        assert_eq!(report.proposed.len(), 2);
    }
}
