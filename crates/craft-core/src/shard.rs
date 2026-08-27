//! Shard routing for write sharding / multi-Raft (ADR 031).
//!
//! v1 runs a **single** Raft group, so every write funnels through one leader
//! and one log — the write-throughput ceiling recorded as risk R1 in ADR 027.
//! The scaling path is to partition the keyspace across **multiple independent
//! Raft groups**, each replicating its own shard of state. That is a large
//! runtime change (N drivers, per-shard storage, cross-shard routing); this
//! module lands the **pure, deterministic routing foundation** it builds on,
//! independently testable and free of any I/O:
//!
//! * [`ShardRouter`] maps an application key to a [`ShardId`] with a stable hash
//!   (so every node in the cluster agrees on the mapping).
//! * [`place_shard`] / [`shard_assignment`] map shards onto Raft groups with
//!   **rendezvous (highest-random-weight) hashing**, so adding or removing a
//!   group relocates a minimal, roughly `1/N` fraction of shards rather than
//!   reshuffling everything.
//!
//! The number of shards is fixed for the life of a cluster (repartitioning is
//! out of scope); groups may come and go, and rendezvous hashing keeps the
//! churn small when they do.
//!
//! Per-group Raft membership planning (desired voter sets, join/leave diffs)
//! lives here too — ADR 033.

use std::collections::BTreeMap;

/// A partition of the keyspace. Fixed count per cluster; each shard is owned by
/// exactly one Raft group at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u32);

/// Identifies one of the cluster's independent Raft groups (multi-Raft).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RaftGroupId(pub u32);

/// Default replication factor for per-group voter sets (ADR 033).
pub const DEFAULT_GROUP_REPLICATION_FACTOR: u32 = 3;

/// FNV-1a (64-bit): a small, dependency-free, **stable** hash. Stability matters
/// because the mapping must be identical on every node and across process
/// restarts — unlike `DefaultHasher`, whose output is not guaranteed stable.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Mix two integers into a well-distributed hash (a `SplitMix64`-style
/// finalizer). Used for rendezvous weights, which need good bit dispersion so
/// shards spread evenly across groups.
fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Maps application keys onto a fixed number of shards with a stable hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRouter {
    shard_count: u32,
}

impl ShardRouter {
    /// A router over `shard_count` shards (clamped to at least 1).
    #[must_use]
    pub fn new(shard_count: u32) -> Self {
        Self {
            shard_count: shard_count.max(1),
        }
    }

    /// The number of shards this router partitions keys into.
    #[must_use]
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// The shard owning `key`, by stable hash modulo the shard count.
    #[must_use]
    pub fn shard_for(&self, key: &[u8]) -> ShardId {
        ShardId((fnv1a(key) % u64::from(self.shard_count)) as u32)
    }
}

/// The Raft group that owns `shard`, chosen by rendezvous (highest-random-weight)
/// hashing: the group maximizing `mix64(shard, group)`. Returns `None` only when
/// `groups` is empty. Deterministic given the same group set, and stable under
/// group churn — removing the winning group promotes the next-highest weight,
/// leaving all other shards' owners unchanged.
#[must_use]
pub fn place_shard(shard: ShardId, groups: &[RaftGroupId]) -> Option<RaftGroupId> {
    groups.iter().copied().max_by_key(|g| weight(shard, *g))
}

/// The rendezvous weight of pairing `shard` with `group`.
fn weight(shard: ShardId, group: RaftGroupId) -> u64 {
    mix64(u64::from(shard.0) << 32 | u64::from(group.0))
}

/// Rendezvous weight for placing `group` on physical node `node`.
fn group_node_weight(group: RaftGroupId, node: craft_proto::NodeId) -> u64 {
    mix64(u64::from(group.0) << 32 | node.0)
}

/// The physical node that should host the sole replica of `group` among
/// `nodes`, by rendezvous hashing. Returns `None` when `nodes` is empty.
/// Deterministic and stable under node churn — the same property as
/// [`place_shard`].
#[must_use]
pub(crate) fn place_group(
    group: RaftGroupId,
    nodes: &[craft_proto::NodeId],
) -> Option<craft_proto::NodeId> {
    nodes
        .iter()
        .copied()
        .max_by_key(|n| group_node_weight(group, *n))
}

/// Full assignment of each Raft group to a host node over `nodes`.
#[must_use]
pub fn group_host_assignment(
    groups: &[RaftGroupId],
    nodes: &[craft_proto::NodeId],
) -> BTreeMap<RaftGroupId, craft_proto::NodeId> {
    let mut map = BTreeMap::new();
    if nodes.is_empty() {
        return map;
    }
    for &group in groups {
        if let Some(node) = place_group(group, nodes) {
            map.insert(group, node);
        }
    }
    map
}

/// Clamp `replication_factor` to `[1, live_count]`. Returns `0` when
/// `live_count == 0`.
#[must_use]
pub fn effective_replication_factor(replication_factor: u32, live_count: usize) -> u32 {
    if live_count == 0 {
        return 0;
    }
    replication_factor.max(1).min(live_count as u32)
}

/// Desired voter set for one Raft group: the top [`effective_replication_factor`]
/// live nodes by rendezvous weight for `group`, sorted by `NodeId` (ADR 033).
#[must_use]
pub fn group_voters(
    group: RaftGroupId,
    live_nodes: &[craft_proto::NodeId],
    replication_factor: u32,
) -> Vec<craft_proto::NodeId> {
    let rf = effective_replication_factor(replication_factor, live_nodes.len());
    if rf == 0 {
        return Vec::new();
    }
    let mut ranked: Vec<_> = live_nodes.to_vec();
    ranked.sort_by(|a, b| {
        group_node_weight(group, *b)
            .cmp(&group_node_weight(group, *a))
            .then_with(|| a.cmp(b))
    });
    ranked.truncate(rf as usize);
    ranked.sort();
    ranked
}

/// Full desired voter assignment for every group in `groups` (ADR 033).
#[must_use]
pub fn group_membership_assignment(
    groups: &[RaftGroupId],
    live_nodes: &[craft_proto::NodeId],
    replication_factor: u32,
) -> BTreeMap<RaftGroupId, Vec<craft_proto::NodeId>> {
    groups
        .iter()
        .map(|&group| (group, group_voters(group, live_nodes, replication_factor)))
        .collect()
}

/// Per-group membership delta between a committed and desired voter set.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupMembershipChange {
    /// Voters to add via joint consensus.
    pub add: Vec<craft_proto::NodeId>,
    /// Voters to remove via joint consensus.
    pub remove: Vec<craft_proto::NodeId>,
}

/// Diff `current_voters` against `desired_voters` (sorted inputs not required).
#[must_use]
pub fn plan_group_membership_change(
    current_voters: &[craft_proto::NodeId],
    desired_voters: &[craft_proto::NodeId],
) -> GroupMembershipChange {
    use std::collections::BTreeSet;

    let current: BTreeSet<_> = current_voters.iter().copied().collect();
    let desired: BTreeSet<_> = desired_voters.iter().copied().collect();
    GroupMembershipChange {
        add: desired.difference(&current).copied().collect(),
        remove: current.difference(&desired).copied().collect(),
    }
}

/// Groups whose desired voter set gains `node` when the live set grows from
/// `live_nodes_before` to `live_nodes_after` (cluster join).
#[must_use]
pub fn groups_joining_node_affects(
    node: craft_proto::NodeId,
    all_groups: &[RaftGroupId],
    live_nodes_before: &[craft_proto::NodeId],
    live_nodes_after: &[craft_proto::NodeId],
    replication_factor: u32,
) -> Vec<RaftGroupId> {
    debug_assert!(
        live_nodes_after.contains(&node),
        "joining node must appear in live_nodes_after"
    );
    debug_assert!(
        !live_nodes_before.contains(&node),
        "joining node must be absent from live_nodes_before"
    );
    all_groups
        .iter()
        .copied()
        .filter(|&group| {
            let before = group_voters(group, live_nodes_before, replication_factor);
            let after = group_voters(group, live_nodes_after, replication_factor);
            !before.contains(&node) && after.contains(&node)
        })
        .collect()
}

/// Groups whose desired voter set loses `node` when it departs the live set
/// (cluster leave).
#[must_use]
pub fn groups_leaving_node_affects(
    node: craft_proto::NodeId,
    all_groups: &[RaftGroupId],
    live_nodes_before: &[craft_proto::NodeId],
    live_nodes_after: &[craft_proto::NodeId],
    replication_factor: u32,
) -> Vec<RaftGroupId> {
    debug_assert!(
        live_nodes_before.contains(&node),
        "departing node must appear in live_nodes_before"
    );
    debug_assert!(
        !live_nodes_after.contains(&node),
        "departing node must be absent from live_nodes_after"
    );
    all_groups
        .iter()
        .copied()
        .filter(|&group| {
            let before = group_voters(group, live_nodes_before, replication_factor);
            let after = group_voters(group, live_nodes_after, replication_factor);
            before.contains(&node) && !after.contains(&node)
        })
        .collect()
}

/// Groups whose desired voter set differs from `current` (ADR 033). Skips
/// coordinator group 0 — its membership is managed by `/cluster/join`.
#[must_use]
pub fn plan_group_membership_sync(
    catalog: &[RaftGroupId],
    live_nodes: &[craft_proto::NodeId],
    current: &BTreeMap<RaftGroupId, Vec<craft_proto::NodeId>>,
    replication_factor: u32,
) -> BTreeMap<RaftGroupId, Vec<craft_proto::NodeId>> {
    let mut out = BTreeMap::new();
    for &group in catalog {
        if group.0 == 0 {
            continue;
        }
        let desired = group_voters(group, live_nodes, replication_factor);
        let cur = current.get(&group).map(Vec::as_slice).unwrap_or(&[]);
        let change = plan_group_membership_change(cur, &desired);
        if !change.add.is_empty() || !change.remove.is_empty() {
            out.insert(group, desired);
        }
    }
    out
}

/// Local rebalance actions for one physical node (multi-Raft control plane).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupRebalancePlan {
    /// Groups this node should begin hosting.
    pub adopt: Vec<RaftGroupId>,
    /// Groups this node should stop hosting.
    pub retire: Vec<RaftGroupId>,
}

/// Diff the groups `node_id` currently hosts against groups where it is in
/// the desired voter set ([`group_voters`](group_voters), ADR 033).
#[must_use]
pub fn plan_node_group_rebalance(
    node_id: craft_proto::NodeId,
    all_groups: &[RaftGroupId],
    live_nodes: &[craft_proto::NodeId],
    currently_hosted: &[RaftGroupId],
    replication_factor: u32,
) -> GroupRebalancePlan {
    use std::collections::BTreeSet;

    let should: BTreeSet<RaftGroupId> = all_groups
        .iter()
        .copied()
        .filter(|g| group_voters(*g, live_nodes, replication_factor).contains(&node_id))
        .collect();
    let current: BTreeSet<RaftGroupId> = currently_hosted.iter().copied().collect();

    let adopt = should.difference(&current).copied().collect();
    let retire = current.difference(&should).copied().collect();
    GroupRebalancePlan { adopt, retire }
}

/// The full shard → owning-group assignment for `shard_count` shards over
/// `groups`, using [`place_shard`]. Empty when `groups` is empty.
#[cfg(test)]
#[must_use]
pub(crate) fn shard_assignment(
    shard_count: u32,
    groups: &[RaftGroupId],
) -> BTreeMap<ShardId, RaftGroupId> {
    let mut map = BTreeMap::new();
    if groups.is_empty() {
        return map;
    }
    for s in 0..shard_count.max(1) {
        let shard = ShardId(s);
        if let Some(group) = place_shard(shard, groups) {
            map.insert(shard, group);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_mapping_is_stable_and_in_range() {
        let router = ShardRouter::new(16);
        // Deterministic: same key → same shard, every time.
        let a = router.shard_for(b"account:42");
        let b = router.shard_for(b"account:42");
        assert_eq!(a, b);
        // Always in `[0, shard_count)`.
        for key in ["a", "bb", "account:1", "account:2", "x/y/z", ""] {
            assert!(router.shard_for(key.as_bytes()).0 < 16);
        }
    }

    #[test]
    fn keys_spread_across_shards() {
        let router = ShardRouter::new(8);
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            seen.insert(router.shard_for(format!("key-{i}").as_bytes()).0);
        }
        // A good hash should touch every shard over 1000 keys.
        assert_eq!(seen.len(), 8, "all shards should receive keys");
    }

    #[test]
    fn single_shard_router_maps_everything_to_zero() {
        let router = ShardRouter::new(1);
        assert_eq!(router.shard_for(b"anything"), ShardId(0));
        assert_eq!(router.shard_for(b"else"), ShardId(0));
    }

    #[test]
    fn placement_is_deterministic_and_covers_only_given_groups() {
        let groups = [RaftGroupId(1), RaftGroupId(2), RaftGroupId(3)];
        let map = shard_assignment(64, &groups);
        assert_eq!(map.len(), 64);
        for group in map.values() {
            assert!(groups.contains(group));
        }
        // Re-running yields the identical assignment.
        assert_eq!(map, shard_assignment(64, &groups));
    }

    #[test]
    fn empty_group_set_places_nothing() {
        assert!(place_shard(ShardId(0), &[]).is_none());
        assert!(shard_assignment(32, &[]).is_empty());
    }

    #[test]
    fn placement_is_roughly_balanced() {
        let groups: Vec<_> = (1..=4).map(RaftGroupId).collect();
        let map = shard_assignment(400, &groups);
        let mut counts = std::collections::HashMap::new();
        for g in map.values() {
            *counts.entry(*g).or_insert(0u32) += 1;
        }
        // Each of 4 groups should own ~100 of 400 shards; allow generous slack.
        for (_g, n) in counts {
            assert!(
                (50..=150).contains(&n),
                "group owned {n} shards (want ~100)"
            );
        }
    }

    #[test]
    fn adding_a_group_moves_a_minimal_fraction_of_shards() {
        // Rendezvous hashing's key property: growing from 3→4 groups should move
        // only shards that the new group now wins — about 1/4 — and never
        // shuffle shards between the pre-existing groups.
        let before = shard_assignment(400, &[RaftGroupId(1), RaftGroupId(2), RaftGroupId(3)]);
        let after = shard_assignment(
            400,
            &[
                RaftGroupId(1),
                RaftGroupId(2),
                RaftGroupId(3),
                RaftGroupId(4),
            ],
        );

        let mut moved = 0;
        for (shard, old) in &before {
            let new = after[shard];
            if new != *old {
                // Any moved shard must have moved *to the new group*, never
                // between two old groups.
                assert_eq!(
                    new,
                    RaftGroupId(4),
                    "shard {shard:?} churned between old groups"
                );
                moved += 1;
            }
        }
        // Expect roughly a quarter to move; assert it stays well under half.
        assert!(moved > 0, "adding a group should claim some shards");
        assert!(
            moved < 200,
            "moved {moved}/400 shards — rendezvous hashing should move ~1/4"
        );
    }

    #[test]
    fn group_hosts_spread_across_physical_nodes() {
        use craft_proto::NodeId;

        let nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let groups: Vec<_> = (0..6).map(RaftGroupId).collect();
        let map = group_host_assignment(&groups, &nodes);
        assert_eq!(map.len(), 6);
        for host in map.values() {
            assert!(nodes.contains(host));
        }
    }

    #[test]
    fn node_rebalance_plan_adopts_for_a_joining_node() {
        use craft_proto::NodeId;

        let groups: Vec<_> = (0..12).map(RaftGroupId).collect();
        let live = [NodeId(1), NodeId(2), NodeId(3)];
        let assignment = group_host_assignment(&groups, &live);
        let node_id = live
            .iter()
            .copied()
            .find(|n| assignment.values().any(|host| *host == *n))
            .expect("at least one node should host a group");
        let plan = plan_node_group_rebalance(node_id, &groups, &live, &[], 1);
        assert!(!plan.adopt.is_empty());
        assert!(plan.retire.is_empty());
    }

    #[test]
    fn effective_replication_factor_clamps_to_live_count() {
        assert_eq!(effective_replication_factor(0, 5), 1);
        assert_eq!(effective_replication_factor(3, 2), 2);
        assert_eq!(effective_replication_factor(3, 5), 3);
        assert_eq!(effective_replication_factor(3, 0), 0);
    }

    #[test]
    fn group_voters_rf_one_matches_rendezvous_host() {
        use craft_proto::NodeId;

        let nodes = [NodeId(1), NodeId(2), NodeId(3)];
        for g in 0..12 {
            let group = RaftGroupId(g);
            let voters = group_voters(group, &nodes, 1);
            assert_eq!(voters.len(), 1);
            assert_eq!(voters[0], place_group(group, &nodes).unwrap());
        }
    }

    #[test]
    fn full_replication_assigns_all_live_nodes_to_every_group() {
        use craft_proto::NodeId;

        let nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let groups: Vec<_> = (0..4).map(RaftGroupId).collect();
        let assignment = group_membership_assignment(&groups, &nodes, 3);
        for voters in assignment.values() {
            assert_eq!(*voters, vec![NodeId(1), NodeId(2), NodeId(3)]);
        }
    }

    #[test]
    fn join_affects_only_groups_where_node_enters_voter_set() {
        use craft_proto::NodeId;

        let groups: Vec<_> = (0..12).map(RaftGroupId).collect();
        let before = [NodeId(1), NodeId(2), NodeId(3)];
        let after = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let affected = groups_joining_node_affects(NodeId(4), &groups, &before, &after, 1);
        assert!(!affected.is_empty());
        assert!(affected.len() < groups.len());
        for g in &affected {
            assert_eq!(group_voters(*g, &after, 1), vec![NodeId(4)]);
        }
        for g in groups.iter().filter(|g| !affected.contains(g)) {
            assert_eq!(
                group_voters(*g, &before, 1),
                group_voters(*g, &after, 1),
                "group {:?} should be unchanged by join",
                g
            );
        }
    }

    #[test]
    fn leave_affects_groups_that_drop_the_departed_node() {
        use craft_proto::NodeId;

        let groups: Vec<_> = (0..12).map(RaftGroupId).collect();
        let before = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let after = [NodeId(1), NodeId(2), NodeId(3)];
        let affected = groups_leaving_node_affects(NodeId(4), &groups, &before, &after, 1);
        for g in &affected {
            let voters = group_voters(*g, &before, 1);
            assert_eq!(voters, vec![NodeId(4)]);
        }
    }

    #[test]
    fn plan_group_membership_change_diffs_add_and_remove() {
        use craft_proto::NodeId;

        let change = plan_group_membership_change(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(4)],
        );
        assert_eq!(change.add, vec![NodeId(4)]);
        assert_eq!(change.remove, vec![NodeId(3)]);
    }

    #[test]
    fn plan_group_membership_sync_skips_coordinator_and_diffs_shards() {
        use craft_proto::NodeId;

        let catalog: Vec<_> = (0..4).map(RaftGroupId).collect();
        let live = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let mut current = BTreeMap::new();
        current.insert(RaftGroupId(0), vec![NodeId(1), NodeId(2), NodeId(3)]);
        current.insert(RaftGroupId(1), vec![NodeId(1), NodeId(2), NodeId(3)]);
        current.insert(RaftGroupId(2), vec![NodeId(1), NodeId(2), NodeId(3)]);

        let sync = plan_group_membership_sync(&catalog, &live, &current, 3);
        assert!(!sync.contains_key(&RaftGroupId(0)));
        assert!(sync.values().all(|v| v.contains(&NodeId(4))));
    }
}
