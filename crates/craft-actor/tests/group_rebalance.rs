//! Multi-Raft group rebalance planner (ADR 031).

use craft_actor::{ClusterState, RaftGroupReconciler};
use craft_core::RaftGroupId;
use craft_proto::NodeId;

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
fn follower_skips_rebalance_planning() {
    let catalog: Vec<_> = (0..4).map(RaftGroupId).collect();
    let reconciler = RaftGroupReconciler::new(
        NodeId(1),
        catalog,
        MockState {
            leader: false,
            live: vec![NodeId(1), NodeId(2)],
        },
    );
    let report = reconciler.reconcile_local(&[RaftGroupId(0), RaftGroupId(1)]);
    assert!(!report.ran_as_leader);
    assert!(report.plan.adopt.is_empty());
    assert!(report.plan.retire.is_empty());
}

#[test]
fn leader_plans_adopt_for_a_new_node() {
    let catalog: Vec<_> = (0..12).map(RaftGroupId).collect();
    let live = vec![NodeId(1), NodeId(2), NodeId(3)];
    let assignment = craft_core::group_host_assignment(&catalog, &live);
    let node_id = live
        .iter()
        .copied()
        .find(|n| assignment.values().any(|host| *host == *n))
        .expect("at least one node should host a group");
    let reconciler = RaftGroupReconciler::new(node_id, catalog, MockState { leader: true, live });
    let report = reconciler.reconcile_local(&[]);
    assert!(report.ran_as_leader);
    assert!(!report.plan.adopt.is_empty());
    assert_eq!(report.assignment.len(), 12);
}
