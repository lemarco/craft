//! Leader-triggered multi-Raft group placement (ADR 031 control plane).

use std::collections::BTreeMap;

use craft_core::{
    GroupRebalancePlan, RaftGroupId, group_host_assignment, plan_node_group_rebalance,
};
use craft_proto::NodeId;

use crate::rebalance_log;
use crate::supervisor::ClusterState;

/// Cluster-wide group → host assignment plus the local adopt/retire plan.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupRebalanceReport {
    /// Whether this node acted as leader (`false` means planning was skipped).
    pub ran_as_leader: bool,
    /// Full rendezvous assignment over the live membership.
    pub assignment: BTreeMap<RaftGroupId, NodeId>,
    /// Local actions for this physical node.
    pub plan: GroupRebalancePlan,
}

/// Computes rendezvous group placement and the local rebalance diff (ADR 031).
pub struct RaftGroupReconciler<S: ClusterState> {
    node_id: NodeId,
    catalog: Vec<RaftGroupId>,
    state: S,
}

impl<S: ClusterState> RaftGroupReconciler<S> {
    /// Plan rebalance for `catalog` groups using leadership/membership from `state`.
    #[must_use]
    pub fn new(node_id: NodeId, catalog: Vec<RaftGroupId>, state: S) -> Self {
        Self {
            node_id,
            catalog,
            state,
        }
    }

    /// Diff locally hosted groups against the desired rendezvous assignment.
    /// Runs on the leader only; followers return an empty plan.
    #[must_use]
    pub fn reconcile_local(&self, currently_hosted: &[RaftGroupId]) -> GroupRebalanceReport {
        if !self.state.is_leader() {
            rebalance_log::skipped_follower(self.node_id);
            return GroupRebalanceReport {
                ran_as_leader: false,
                ..GroupRebalanceReport::default()
            };
        }
        let live = self.state.live_nodes();
        let assignment = group_host_assignment(&self.catalog, &live);
        let plan = plan_node_group_rebalance(self.node_id, &self.catalog, &live, currently_hosted);
        rebalance_log::plan(self.node_id, &live, currently_hosted, &plan);
        GroupRebalanceReport {
            ran_as_leader: true,
            assignment,
            plan,
        }
    }
}
