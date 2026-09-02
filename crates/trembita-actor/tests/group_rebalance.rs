//! Multi-Raft group rebalance planner (write-sharding-multi-raft).

use trembita_actor::{ClusterState, RaftGroupReconciler};
use trembita_core::RaftGroupId;
use trembita_proto::NodeId;

struct MockState {
    leader: bool,
    live: Vec<NodeId>,
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        self.leader
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.live.clone()
    }
}

#[test]
fn every_node_plans_local_adopt_and_retire() {
    let catalog: Vec<_> = (0..4).map(RaftGroupId).collect();
    let reconciler = RaftGroupReconciler::new(
        NodeId(1),
        catalog,
        1,
        0,
        MockState {
            leader: false,
            live: vec![NodeId(1), NodeId(2)],
        },
    );
    let report = reconciler.reconcile_local(&[RaftGroupId(0), RaftGroupId(1)]);
    assert!(!report.ran_as_leader);
    assert!(!report.plan.adopt.is_empty() || !report.plan.retire.is_empty());
}

#[test]
fn leader_plans_adopt_for_a_new_node() {
    let catalog: Vec<_> = (0..12).map(RaftGroupId).collect();
    let live = vec![NodeId(1), NodeId(2), NodeId(3)];
    let assignment = trembita_core::group_host_assignment(&catalog, &live);
    let node_id = live
        .iter()
        .copied()
        .find(|n| assignment.values().any(|host| *host == *n))
        .expect("at least one node should host a group");
    let reconciler =
        RaftGroupReconciler::new(node_id, catalog, 1, 0, MockState { leader: true, live });
    let report = reconciler.reconcile_local(&[]);
    assert!(report.ran_as_leader);
    assert!(!report.plan.adopt.is_empty());
    assert_eq!(report.assignment.len(), 12);
}

#[test]
fn learner_only_node_adopts_groups_when_lf_positive() {
    let catalog: Vec<_> = (0..8).map(RaftGroupId).collect();
    let live = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let learner = NodeId(4);
    let mut learner_only = false;
    for group in &catalog {
        let voters = trembita_core::group_voters(*group, &live, 3);
        let learners = trembita_core::group_learners(*group, &live, 3, 1);
        if learners.contains(&learner) && !voters.contains(&learner) {
            learner_only = true;
            break;
        }
    }
    assert!(
        learner_only,
        "node 4 should be learner-only for some groups"
    );

    let reconciler = RaftGroupReconciler::new(
        learner,
        catalog,
        3,
        1,
        MockState {
            leader: false,
            live: live.clone(),
        },
    );
    let report = reconciler.reconcile_local(&[]);
    assert!(
        !report.plan.adopt.is_empty(),
        "learner node should adopt groups: {:?}",
        report.plan
    );
}

#[test]
fn fourth_node_join_plans_group_adopts() {
    let catalog: Vec<_> = (0..12).map(RaftGroupId).collect();
    let live = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let reconciler = RaftGroupReconciler::new(
        NodeId(4),
        catalog,
        3,
        0,
        MockState {
            leader: true,
            live: live.clone(),
        },
    );
    let report = reconciler.reconcile_local(&[]);
    assert!(report.ran_as_leader);
    assert!(
        !report.plan.adopt.is_empty(),
        "new node should adopt groups from existing hosts: {:?}",
        report.plan
    );
    assert_eq!(report.assignment.len(), 12);
}
