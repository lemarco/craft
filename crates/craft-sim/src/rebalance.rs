//! Multi-Raft group hosting simulation under live-node churn.
//!
//! Models per-node adopt/retire via [`plan_node_group_rebalance`] without a full
//! runtime — the same pure planner the supervisor uses (write-sharding-multi-raft).

use std::collections::{BTreeMap, BTreeSet};

use craft_core::{
    RaftGroupId, effective_replication_factor, group_learners, group_voters,
    node_should_host_group, plan_node_group_rebalance,
};
use craft_proto::NodeId;

/// Simulated cluster hosting state: which physical nodes run which groups.
pub struct RebalanceSim {
    catalog: Vec<RaftGroupId>,
    replication_factor: u32,
    learner_factor: u32,
    all_nodes: Vec<NodeId>,
    live: BTreeSet<NodeId>,
    hosted: BTreeMap<NodeId, BTreeSet<RaftGroupId>>,
}

impl RebalanceSim {
    /// All node ids `1..=max_nodes`, full live set, empty hosting — then one
    /// reconcile pass to converge.
    ///
    /// # Panics
    /// Panics if `max_nodes` or `group_count` is zero.
    #[must_use]
    pub fn new(
        max_nodes: u64,
        group_count: u32,
        replication_factor: u32,
        learner_factor: u32,
    ) -> Self {
        assert!(max_nodes >= 1, "need at least one node");
        assert!(group_count >= 1, "need at least one group");
        let all_nodes: Vec<_> = (1..=max_nodes).map(NodeId).collect();
        let catalog: Vec<_> = (0..group_count).map(RaftGroupId).collect();
        let live: BTreeSet<_> = all_nodes.iter().copied().collect();
        let mut sim = Self {
            catalog,
            replication_factor,
            learner_factor,
            all_nodes,
            live,
            hosted: BTreeMap::new(),
        };
        sim.reconcile_all();
        sim
    }

    /// Live membership after a join/leave event.
    #[must_use]
    pub fn live_nodes(&self) -> Vec<u64> {
        let mut v: Vec<_> = self.live.iter().map(|n| n.0).collect();
        v.sort_unstable();
        v
    }

    /// Groups hosted on `node`.
    #[must_use]
    pub fn hosted_groups(&self, node: u64) -> Vec<u32> {
        self.hosted
            .get(&NodeId(node))
            .map(|s| s.iter().map(|g| g.0).collect())
            .unwrap_or_default()
    }

    /// Replace the live set and reconcile every node (supervisor tick).
    pub fn set_live_ids(&mut self, live: &[u64]) {
        self.live = live.iter().map(|n| NodeId(*n)).collect();
        for id in &self.all_nodes {
            if !self.live.contains(id) {
                self.hosted.remove(id);
            }
        }
        self.reconcile_all();
    }

    /// Apply one local adopt/retire pass for every known physical node.
    pub fn reconcile_all(&mut self) {
        let live = self.live_vec();
        for &node in &self.all_nodes {
            if !self.live.contains(&node) {
                continue;
            }
            let current: Vec<_> = self
                .hosted
                .get(&node)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();
            let plan = plan_node_group_rebalance(
                node,
                &self.catalog,
                &live,
                &current,
                self.replication_factor,
                self.learner_factor,
            );
            let set = self.hosted.entry(node).or_default();
            for group in plan.retire {
                set.remove(&group);
            }
            for group in plan.adopt {
                set.insert(group);
            }
        }
    }

    /// Every live node hosts exactly the planner's desired groups; every voter
    /// and learner replica is hosted somewhere.
    ///
    /// # Panics
    /// Panics when hosting state violates planner invariants.
    pub fn assert_hosting_invariants(&self) {
        let live = self.live_vec();
        let rf = effective_replication_factor(self.replication_factor, live.len());
        assert!(rf >= 1, "live set too small for replication factor");

        for &node in &self.live {
            let hosted = self.hosted.get(&node).cloned().unwrap_or_default();
            for &group in &self.catalog {
                let should = node_should_host_group(
                    group,
                    node,
                    &live,
                    self.replication_factor,
                    self.learner_factor,
                );
                assert_eq!(
                    hosted.contains(&group),
                    should,
                    "node {} group {} hosting mismatch (live={live:?})",
                    node.0,
                    group.0
                );
            }
        }

        for &group in &self.catalog {
            for voter in group_voters(group, &live, self.replication_factor) {
                assert!(
                    self.hosted.get(&voter).is_some_and(|h| h.contains(&group)),
                    "voter {} must host group {} (live={live:?})",
                    voter.0,
                    group.0
                );
            }
            for learner in
                group_learners(group, &live, self.replication_factor, self.learner_factor)
            {
                assert!(
                    self.hosted
                        .get(&learner)
                        .is_some_and(|h| h.contains(&group)),
                    "learner {} must host group {} (live={live:?})",
                    learner.0,
                    group.0
                );
            }
        }
    }

    fn live_vec(&self) -> Vec<NodeId> {
        let mut v: Vec<_> = self.live.iter().copied().collect();
        v.sort();
        v
    }
}
