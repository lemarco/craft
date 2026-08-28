//! Multi-Raft simulation — independent Raft groups with shard-aware routing.

use craft_core::{RaftGroupId, ShardRouter, place_shard};
use craft_proto::TwoPhasePrepareCommand;

use crate::harness::{Cluster, Fault};

/// Several independent [`Cluster`] instances keyed by Raft group, with
/// shard-aware proposal routing (write-sharding-multi-raft).
pub struct MultiRaftCluster {
    group_ids: Vec<RaftGroupId>,
    router: ShardRouter,
    groups: Vec<Cluster>,
}

impl MultiRaftCluster {
    /// Build `group_count` independent Raft clusters over `nodes` processes each.
    ///
    /// # Panics
    /// Panics if `group_count` is zero.
    #[must_use]
    pub fn new(nodes: u64, group_count: u32, seed: u64) -> Self {
        assert!(group_count >= 1, "multi-raft sim needs at least one group");
        let group_ids: Vec<RaftGroupId> = (0..group_count).map(RaftGroupId).collect();
        let groups = group_ids
            .iter()
            .map(|g| Cluster::new(nodes, seed ^ u64::from(g.0).wrapping_mul(0x9E37_79B9)))
            .collect();
        Self {
            group_ids,
            router: ShardRouter::new(64),
            groups,
        }
    }

    /// Override the network fault profile for every hosted group.
    pub fn set_fault(&mut self, fault: Fault) {
        for group in &mut self.groups {
            group.set_fault(fault);
        }
    }

    /// Partition every group the same way.
    pub fn isolate(&mut self, id: u64) {
        for group in &mut self.groups {
            group.isolate(id);
        }
    }

    /// Heal partitions in every group.
    pub fn heal(&mut self) {
        for group in &mut self.groups {
            group.heal();
        }
    }

    /// Run all groups forward for `steps` ticks.
    pub fn run(&mut self, steps: u64) {
        for group in &mut self.groups {
            group.run(steps);
        }
    }

    /// Step until every group has a leader or `max` ticks elapse.
    pub fn run_until_leaders(&mut self, max: u64) -> bool {
        for _ in 0..max {
            self.run(1);
            if self.groups.iter().all(|g| g.leader().is_some()) {
                return true;
            }
        }
        self.groups.iter().all(|g| g.leader().is_some())
    }

    /// Route `key` to a group and propose `command` on that group's cluster.
    pub fn propose_keyed(&mut self, key: &[u8], command: Vec<u8>) -> bool {
        let Some(group_idx) = self.group_index_for_key(key) else {
            return false;
        };
        self.groups[group_idx].propose(command)
    }

    /// Whether any node in `group` applied `command`.
    #[must_use]
    pub fn group_applied_any(&self, group: u32, command: &[u8]) -> bool {
        self.groups[group as usize]
            .ids()
            .into_iter()
            .any(|node| self.applied(group, node).iter().any(|c| c == command))
    }

    /// Applied commands at `node` in `group`.
    #[must_use]
    pub fn applied(&self, group: u32, node: u64) -> Vec<Vec<u8>> {
        self.groups[group as usize].applied(node)
    }

    /// Leader node id in `group`, if any.
    #[must_use]
    pub fn leader(&self, group: u32) -> Option<u64> {
        self.groups[group as usize].leader()
    }

    fn group_index_for_key(&self, key: &[u8]) -> Option<usize> {
        let shard = self.router.shard_for(key);
        let group = place_shard(shard, &self.group_ids)?;
        Some(group.0 as usize)
    }

    /// Propose a durable 2PC prepare on the group that owns `key`.
    pub fn propose_two_phase_prepare(
        &mut self,
        group: u32,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        command: Vec<u8>,
    ) -> bool {
        let idx = group as usize;
        if idx >= self.groups.len() {
            return false;
        }
        self.groups[idx].propose_two_phase_prepare(TwoPhasePrepareCommand {
            tx_id,
            route_key,
            command,
            prepared_at_ms: 0,
        })
    }

    /// Whether `group` committed a durable prepare for `(tx_id, route_key)`.
    #[must_use]
    pub fn group_has_two_phase_prepare(&self, group: u32, tx_id: &[u8], route_key: &[u8]) -> bool {
        self.groups[group as usize]
            .two_phase_prepares()
            .iter()
            .any(|p| p.tx_id == tx_id && p.route_key == route_key)
    }
}
