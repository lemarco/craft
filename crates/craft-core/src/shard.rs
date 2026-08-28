//! Shard routing for write sharding / multi-Raft (write-sharding-multi-raft).
//!
//! v1 runs a **single** Raft group, so every write funnels through one leader
//! and one log — the write-throughput ceiling recorded as risk R1 in future-work-and-risks.
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
//! lives here too — per-group-raft-membership.

use std::collections::{BTreeMap, BTreeSet};

/// A partition of the keyspace. Fixed count per cluster; each shard is owned by
/// exactly one Raft group at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u32);

/// Identifies one of the cluster's independent Raft groups (multi-Raft).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RaftGroupId(pub u32);

/// Reserved Raft group id for the cluster coordinator (Meta-Raft).
///
/// Hosts cluster registry (join/leave), dynamic catalog, and saga journal metadata.
/// Not part of the user catalog or keyed shard routing.
pub const META_RAFT_GROUP_ID: u32 = u32::MAX;

/// Whether `group` is the Meta-Raft coordinator group.
#[must_use]
pub const fn is_meta_raft_group(group: u32) -> bool {
    group == META_RAFT_GROUP_ID
}

/// Default replication factor for per-group voter sets (per-group-raft-membership).
pub const DEFAULT_GROUP_REPLICATION_FACTOR: u32 = 3;

/// Default non-voting learner replicas per group beyond voters (Tier 1). `0` disables.
pub const DEFAULT_GROUP_LEARNER_FACTOR: u32 = 0;

/// Upper bound for [`ShardRouter`] active shard counts (Tier 1 expansion).
pub const MAX_VIRTUAL_SHARDS: u32 = 4096;

/// How keyed traffic maps into the virtual shard space (Tier 1 vs Tier 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRoutingKind {
    /// Tier 1: `hash(key) % active_count` — keys remap when the count grows.
    Modulus,
    /// Tier 2: fixed virtual id `hash(key) % MAX_VIRTUAL_SHARDS` with an active prefix.
    StableVirtual,
}

impl ShardRoutingKind {
    /// Stable string for introspect / operator tooling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Modulus => "modulus",
            Self::StableVirtual => "stable_virtual",
        }
    }
}

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
    /// A router over `shard_count` shards (clamped to `[1, MAX_VIRTUAL_SHARDS]`).
    #[must_use]
    pub fn new(shard_count: u32) -> Self {
        Self {
            shard_count: shard_count.clamp(1, MAX_VIRTUAL_SHARDS),
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
        #[allow(clippy::cast_possible_truncation)] // hash modulo shard_count always fits u32
        ShardId((fnv1a(key) % u64::from(self.shard_count)) as u32)
    }

    /// Increase the active shard count (operator-driven expansion). Keys
    /// **remap** when the modulus changes — drain clients before applying.
    ///
    /// # Errors
    /// Returns an error when `new_count` shrinks the space or exceeds
    /// [`MAX_VIRTUAL_SHARDS`].
    pub fn expand_shard_count(
        &mut self,
        new_count: u32,
    ) -> Result<ShardCountExpansionPlan, ShardExpansionError> {
        let plan = plan_shard_count_expansion(self.shard_count, new_count)?;
        self.shard_count = plan.to;
        Ok(plan)
    }
}

/// Why a shard-count expansion request was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardExpansionError {
    /// `new_count` must be strictly greater than the current count.
    CannotShrink {
        /// Current active shard count.
        current: u32,
        /// Requested count.
        requested: u32,
    },
    /// `new_count` exceeds [`MAX_VIRTUAL_SHARDS`].
    ExceedsMax {
        /// Requested count.
        requested: u32,
    },
    /// Keyed routing / expansion requires multi-Raft (`raft_groups > 1`).
    NotMultiRaft,
    /// Stable virtual routing is active — use [`StableShardRouter::activate_shards`].
    StableRoutingActive,
}

impl std::fmt::Display for ShardExpansionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotShrink { current, requested } => write!(
                f,
                "shard count can only increase (have {current}, requested {requested})"
            ),
            Self::ExceedsMax { requested } => write!(
                f,
                "requested {requested} shards exceeds MAX_VIRTUAL_SHARDS ({MAX_VIRTUAL_SHARDS})"
            ),
            Self::NotMultiRaft => {
                f.write_str("shard expansion requires multi-Raft (raft_groups > 1)")
            }
            Self::StableRoutingActive => f.write_str(
                "stable virtual shard routing is active; use activate_shards instead of expand_shard_count",
            ),
        }
    }
}

impl std::error::Error for ShardExpansionError {}

/// Plan for expanding the active shard keyspace (Tier 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardCountExpansionPlan {
    /// Previous active shard count.
    pub from: u32,
    /// New active shard count.
    pub to: u32,
    /// Shard ids entering the active range `[from, to)`.
    pub new_shard_ids: Vec<ShardId>,
}

/// Plan a shard-count increase. Shrinking is rejected — pick a larger
/// [`MAX_VIRTUAL_SHARDS`] up front or migrate data explicitly.
///
/// # Errors
/// Returns [`ShardExpansionError`] when `to` is not a strict increase within
/// [`MAX_VIRTUAL_SHARDS`].
pub fn plan_shard_count_expansion(
    from: u32,
    to: u32,
) -> Result<ShardCountExpansionPlan, ShardExpansionError> {
    let from = from.max(1);
    if to <= from {
        return Err(ShardExpansionError::CannotShrink {
            current: from,
            requested: to,
        });
    }
    if to > MAX_VIRTUAL_SHARDS {
        return Err(ShardExpansionError::ExceedsMax { requested: to });
    }
    Ok(ShardCountExpansionPlan {
        from,
        to,
        new_shard_ids: (from..to).map(ShardId).collect(),
    })
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
    replication_factor
        .max(1)
        .min(u32::try_from(live_count).unwrap_or(u32::MAX))
}

/// Desired voter set for one Raft group: the top [`effective_replication_factor`]
/// live nodes by rendezvous weight for `group`, sorted by `NodeId` (per-group-raft-membership).
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

/// Desired learner set for one Raft group: live nodes ranked after the voter
/// set, up to `learner_factor` nodes (Tier 1 per-group-raft-membership).
#[must_use]
pub fn group_learners(
    group: RaftGroupId,
    live_nodes: &[craft_proto::NodeId],
    replication_factor: u32,
    learner_factor: u32,
) -> Vec<craft_proto::NodeId> {
    if learner_factor == 0 || live_nodes.is_empty() {
        return Vec::new();
    }

    let voters: BTreeSet<_> = group_voters(group, live_nodes, replication_factor)
        .into_iter()
        .collect();
    let mut ranked: Vec<_> = live_nodes.to_vec();
    ranked.sort_by(|a, b| {
        group_node_weight(group, *b)
            .cmp(&group_node_weight(group, *a))
            .then_with(|| a.cmp(b))
    });
    ranked.retain(|n| !voters.contains(n));
    ranked.truncate(learner_factor as usize);
    ranked.sort();
    ranked
}

/// Desired voters + learners for one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupReplicationTarget {
    /// Joint-consensus voters.
    pub voters: Vec<craft_proto::NodeId>,
    /// Non-voting learners (catch-up replicas).
    pub learners: Vec<craft_proto::NodeId>,
}

/// Full desired voter assignment for every group in `groups` (per-group-raft-membership).
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

/// Groups whose desired voter/learner sets differ from `current`
/// (per-group-raft-membership). Skips the Meta-Raft coordinator — its
/// membership is managed by `/cluster/join` and `/cluster/leave`.
#[must_use]
pub fn plan_group_membership_sync(
    catalog: &[RaftGroupId],
    live_nodes: &[craft_proto::NodeId],
    current_voters: &BTreeMap<RaftGroupId, Vec<craft_proto::NodeId>>,
    current_learners: &BTreeMap<RaftGroupId, Vec<craft_proto::NodeId>>,
    replication_factor: u32,
    learner_factor: u32,
) -> BTreeMap<RaftGroupId, GroupReplicationTarget> {
    let mut out = BTreeMap::new();
    for &group in catalog {
        if is_meta_raft_group(group.0) {
            continue;
        }
        let desired_voters = group_voters(group, live_nodes, replication_factor);
        let desired_learners =
            group_learners(group, live_nodes, replication_factor, learner_factor);
        let cur_v = current_voters.get(&group).map_or(&[][..], Vec::as_slice);
        let cur_l = current_learners.get(&group).map_or(&[][..], Vec::as_slice);
        let voter_change = plan_group_membership_change(cur_v, &desired_voters);
        let learner_change = plan_group_membership_change(cur_l, &desired_learners);
        if !voter_change.add.is_empty()
            || !voter_change.remove.is_empty()
            || !learner_change.add.is_empty()
            || !learner_change.remove.is_empty()
        {
            out.insert(
                group,
                GroupReplicationTarget {
                    voters: desired_voters,
                    learners: desired_learners,
                },
            );
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

/// Whether `node_id` should run a local replica for `group` (voter or learner).
#[must_use]
pub fn node_should_host_group(
    group: RaftGroupId,
    node_id: craft_proto::NodeId,
    live_nodes: &[craft_proto::NodeId],
    replication_factor: u32,
    learner_factor: u32,
) -> bool {
    group_voters(group, live_nodes, replication_factor).contains(&node_id)
        || group_learners(group, live_nodes, replication_factor, learner_factor).contains(&node_id)
}

/// Diff the groups `node_id` currently hosts against groups where it belongs
/// in the desired voter or learner set (per-group-raft-membership).
#[must_use]
pub fn plan_node_group_rebalance(
    node_id: craft_proto::NodeId,
    all_groups: &[RaftGroupId],
    live_nodes: &[craft_proto::NodeId],
    currently_hosted: &[RaftGroupId],
    replication_factor: u32,
    learner_factor: u32,
) -> GroupRebalancePlan {
    use std::collections::BTreeSet;

    let should: BTreeSet<RaftGroupId> = all_groups
        .iter()
        .copied()
        .filter(|g| {
            node_should_host_group(*g, node_id, live_nodes, replication_factor, learner_factor)
        })
        .collect();
    let current: BTreeSet<RaftGroupId> = currently_hosted.iter().copied().collect();

    let adopt = should.difference(&current).copied().collect();
    let retire = current.difference(&should).copied().collect();
    GroupRebalancePlan { adopt, retire }
}

// ---------------------------------------------------------------------------
// Tier 2 — stable virtual shards + dynamic catalog (pure planners)
// ---------------------------------------------------------------------------

/// Map `key` to a **fixed** virtual shard in `[0, [``MAX_VIRTUAL_SHARDS``])`.
/// Unlike [`ShardRouter::shard_for`], this id never changes when the active
/// prefix grows ([tier2-multi-raft-architecture]).
///
/// [multi-raft]: ../../../docs/decisions/multi-raft.md
#[must_use]
pub fn virtual_shard_for(key: &[u8]) -> ShardId {
    #[allow(clippy::cast_possible_truncation)] // hash modulo MAX_VIRTUAL_SHARDS always fits u32
    ShardId((fnv1a(key) % u64::from(MAX_VIRTUAL_SHARDS)) as u32)
}

/// Whether `shard` is routable given `active_count` active virtual shards.
#[must_use]
pub fn shard_is_active(shard: ShardId, active_count: u32) -> bool {
    shard.0 < active_count.clamp(1, MAX_VIRTUAL_SHARDS)
}

/// Why stable shard activation was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StableShardActivationError {
    /// `new_active` must be strictly greater than the current active count.
    CannotShrink {
        /// Current active virtual shards.
        current: u32,
        /// Requested active count.
        requested: u32,
    },
    /// `new_active` exceeds [`MAX_VIRTUAL_SHARDS`].
    ExceedsMax {
        /// Requested active count.
        requested: u32,
    },
    /// Tier 1 modulus routing is active — use [`ShardRouter::expand_shard_count`].
    ModulusRoutingActive,
    /// Activation requires multi-Raft (`raft_groups > 1`).
    NotMultiRaft,
}

impl std::fmt::Display for StableShardActivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotShrink { current, requested } => write!(
                f,
                "active shard count can only increase (have {current}, requested {requested})"
            ),
            Self::ExceedsMax { requested } => write!(
                f,
                "requested {requested} active shards exceeds MAX_VIRTUAL_SHARDS ({MAX_VIRTUAL_SHARDS})"
            ),
            Self::ModulusRoutingActive => f.write_str(
                "modulus shard routing is active; use expand_shard_count instead of activate_shards",
            ),
            Self::NotMultiRaft => {
                f.write_str("shard activation requires multi-Raft (raft_groups > 1)")
            }
        }
    }
}

impl std::error::Error for StableShardActivationError {}

/// Plan for activating more virtual shards without remapping existing keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableShardActivationPlan {
    /// Previous active virtual shard count.
    pub from: u32,
    /// New active virtual shard count.
    pub to: u32,
    /// Virtual shard ids entering the active range `[from, to)`.
    pub newly_active: Vec<ShardId>,
}

/// Plan increasing the active virtual shard prefix (Tier 2 stable expansion).
///
/// # Errors
/// Returns [`StableShardActivationError`] when `to` is not a strict increase
/// within [`MAX_VIRTUAL_SHARDS`].
pub fn plan_stable_shard_activation(
    from: u32,
    to: u32,
) -> Result<StableShardActivationPlan, StableShardActivationError> {
    let from = from.clamp(1, MAX_VIRTUAL_SHARDS);
    if to <= from {
        return Err(StableShardActivationError::CannotShrink {
            current: from,
            requested: to,
        });
    }
    if to > MAX_VIRTUAL_SHARDS {
        return Err(StableShardActivationError::ExceedsMax { requested: to });
    }
    Ok(StableShardActivationPlan {
        from,
        to,
        newly_active: (from..to).map(ShardId).collect(),
    })
}

/// Why a Tier 1 → Tier 2 routing switch was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRoutingSwitchError {
    /// Stable virtual routing is already active.
    AlreadyStable,
    /// Active shard count must be at least 1.
    InvalidActiveCount,
    /// Routing switch requires multi-Raft (`raft_groups > 1`).
    NotMultiRaft,
}

impl std::fmt::Display for ShardRoutingSwitchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyStable => {
                f.write_str("stable virtual shard routing is already active; switch is a no-op")
            }
            Self::InvalidActiveCount => {
                f.write_str("active shard count must be at least 1 to switch routing")
            }
            Self::NotMultiRaft => {
                f.write_str("routing switch requires multi-Raft (raft_groups > 1)")
            }
        }
    }
}

impl std::error::Error for ShardRoutingSwitchError {}

/// Operator plan for switching keyed routing from modulus to stable virtual.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRoutingSwitchPlan {
    /// Previous routing mode (always [`ShardRoutingKind::Modulus`]).
    pub from: ShardRoutingKind,
    /// Target routing mode (always [`ShardRoutingKind::StableVirtual`]).
    pub to: ShardRoutingKind,
    /// Active shard count preserved across the switch.
    pub active_count: u32,
}

/// Validate switching from Tier 1 modulus to Tier 2 stable virtual routing.
///
/// Keys **remap** to the stable formula — drain keyed clients before applying
/// ([multi-raft](../../docs/decisions/multi-raft.md)).
///
/// # Errors
/// Returns [`ShardRoutingSwitchError::AlreadyStable`] when already on stable routing.
pub fn plan_switch_to_stable_routing(
    current: ShardRoutingKind,
    active_count: u32,
) -> Result<ShardRoutingSwitchPlan, ShardRoutingSwitchError> {
    if current == ShardRoutingKind::StableVirtual {
        return Err(ShardRoutingSwitchError::AlreadyStable);
    }
    if active_count == 0 {
        return Err(ShardRoutingSwitchError::InvalidActiveCount);
    }
    Ok(ShardRoutingSwitchPlan {
        from: ShardRoutingKind::Modulus,
        to: ShardRoutingKind::StableVirtual,
        active_count: active_count.clamp(1, MAX_VIRTUAL_SHARDS),
    })
}

/// Router over a fixed virtual space with a tunable active prefix (Tier 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableShardRouter {
    active_count: u32,
}

impl StableShardRouter {
    /// Active virtual shard count in `[1, MAX_VIRTUAL_SHARDS]`.
    #[must_use]
    pub fn new(active_count: u32) -> Self {
        Self {
            active_count: active_count.clamp(1, MAX_VIRTUAL_SHARDS),
        }
    }

    /// Number of virtual shards currently accepting keyed traffic.
    #[must_use]
    pub fn active_count(&self) -> u32 {
        self.active_count
    }

    /// Virtual shard for `key`, or `None` when the key lands outside the active prefix.
    #[must_use]
    pub fn shard_for(&self, key: &[u8]) -> Option<ShardId> {
        let shard = virtual_shard_for(key);
        shard_is_active(shard, self.active_count).then_some(shard)
    }

    /// Grow the active prefix without remapping keys already in `[0, from)`.
    ///
    /// # Errors
    /// Same rules as [`plan_stable_shard_activation`].
    pub fn activate_shards(
        &mut self,
        new_active: u32,
    ) -> Result<StableShardActivationPlan, StableShardActivationError> {
        let plan = plan_stable_shard_activation(self.active_count, new_active)?;
        self.active_count = plan.to;
        Ok(plan)
    }
}

/// Invalid multi-Raft group catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogError {
    /// Catalog must not be empty.
    Empty,
    /// Group ids must be contiguous `0..=max` without gaps.
    NonContiguous {
        /// Last valid id before the gap.
        expected_next: u32,
        /// Id that broke contiguity.
        found: u32,
    },
    /// Duplicate group id.
    Duplicate {
        /// Repeated id.
        group: u32,
    },
    /// `add_groups` must be at least 1.
    InvalidExpansionCount {
        /// Requested append count.
        add_groups: u32,
    },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("catalog must not be empty"),
            Self::NonContiguous {
                expected_next,
                found,
            } => write!(
                f,
                "catalog ids must be contiguous (expected {expected_next}, found {found})"
            ),
            Self::Duplicate { group } => write!(f, "duplicate group id {group}"),
            Self::InvalidExpansionCount { add_groups } => {
                write!(f, "add_groups must be >= 1 (got {add_groups})")
            }
        }
    }
}

impl std::error::Error for CatalogError {}

/// Plan for appending contiguous Raft groups to the catalog (Tier 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogExpansionPlan {
    /// Previous catalog length.
    pub from_len: u32,
    /// Catalog length after expansion.
    pub to_len: u32,
    /// New group ids appended in order.
    pub new_groups: Vec<RaftGroupId>,
}

/// Validate a multi-Raft user group catalog (contiguous ids `0..=max`).
///
/// The Meta-Raft coordinator group is not part of the catalog.
///
/// # Errors
/// Returns [`CatalogError`] when `catalog` violates catalog invariants.
pub fn validate_catalog(catalog: &[RaftGroupId]) -> Result<(), CatalogError> {
    if catalog.is_empty() {
        return Err(CatalogError::Empty);
    }
    let mut seen = std::collections::BTreeSet::new();
    for (i, &group) in catalog.iter().enumerate() {
        if !seen.insert(group.0) {
            return Err(CatalogError::Duplicate { group: group.0 });
        }
        #[allow(clippy::cast_possible_truncation)] // catalog indices are contiguous from zero
        let expected = i as u32;
        if group.0 != expected {
            return Err(CatalogError::NonContiguous {
                expected_next: expected,
                found: group.0,
            });
        }
    }
    Ok(())
}

/// Plan appending `add_groups` contiguous ids after the current catalog tail.
///
/// # Errors
/// Returns [`CatalogError`] when the current catalog is invalid or `add_groups`
/// is zero (use `add_groups >= 1`).
///
/// # Panics
/// Panics if the validated catalog is empty (invariant after [`validate_catalog`]).
pub fn plan_catalog_expansion(
    catalog: &[RaftGroupId],
    add_groups: u32,
) -> Result<CatalogExpansionPlan, CatalogError> {
    validate_catalog(catalog)?;
    if add_groups == 0 {
        return Err(CatalogError::InvalidExpansionCount { add_groups });
    }
    let from_len = u32::try_from(catalog.len()).expect("catalog length fits u32");
    let next_id = catalog.last().expect("non-empty").0 + 1;
    let new_groups: Vec<_> = (0..add_groups)
        .map(|offset| RaftGroupId(next_id + offset))
        .collect();
    Ok(CatalogExpansionPlan {
        from_len,
        to_len: from_len + add_groups,
        new_groups,
    })
}

/// After growing the active prefix, keys that were already routable keep the
/// same virtual shard id.
#[must_use]
pub fn stable_router_preserves_routable_keys(
    from_active: u32,
    to_active: u32,
    samples: &[&[u8]],
) -> bool {
    let before = StableShardRouter::new(from_active);
    let mut after = StableShardRouter::new(from_active);
    if after.activate_shards(to_active).is_err() {
        return false;
    }
    for key in samples {
        let before_shard = before.shard_for(key);
        let after_shard = after.shard_for(key);
        if let Some(a) = before_shard
            && after_shard != Some(a)
        {
            return false;
        }
    }
    true
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
        let plan = plan_node_group_rebalance(node_id, &groups, &live, &[], 1, 0);
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
                "group {g:?} should be unchanged by join"
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
    fn plan_group_membership_sync_skips_meta_and_diffs_shards() {
        use craft_proto::NodeId;

        let catalog: Vec<_> = (0..4).map(RaftGroupId).collect();
        let live = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let mut current_voters = BTreeMap::new();
        current_voters.insert(RaftGroupId(0), vec![NodeId(1), NodeId(2), NodeId(3)]);
        current_voters.insert(RaftGroupId(1), vec![NodeId(1), NodeId(2), NodeId(3)]);
        current_voters.insert(RaftGroupId(2), vec![NodeId(1), NodeId(2), NodeId(3)]);
        current_voters.insert(
            RaftGroupId(META_RAFT_GROUP_ID),
            vec![NodeId(1), NodeId(2), NodeId(3)],
        );

        let sync =
            plan_group_membership_sync(&catalog, &live, &current_voters, &BTreeMap::new(), 3, 0);
        assert!(!sync.contains_key(&RaftGroupId(META_RAFT_GROUP_ID)));
        assert!(sync.values().all(|t| t.voters.contains(&NodeId(4))));
    }

    #[test]
    fn group_learners_picks_nodes_after_voters() {
        use craft_proto::NodeId;

        let nodes = [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
        let voters = group_voters(RaftGroupId(1), &nodes, 3);
        let learners = group_learners(RaftGroupId(1), &nodes, 3, 2);
        assert_eq!(learners.len(), 2);
        for l in &learners {
            assert!(!voters.contains(l));
        }
    }

    #[test]
    fn plan_rebalance_keeps_learner_hosted_groups() {
        use craft_proto::NodeId;

        let groups: Vec<_> = (0..4).map(RaftGroupId).collect();
        let live = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let learner = NodeId(4);
        let mut found_learner_only = false;
        for group in &groups {
            let voters = group_voters(*group, &live, 3);
            let learners = group_learners(*group, &live, 3, 1);
            if learners.contains(&learner) && !voters.contains(&learner) {
                found_learner_only = true;
                let without = plan_node_group_rebalance(learner, &groups, &live, &[], 3, 0);
                assert!(
                    !without.adopt.contains(group),
                    "voter-only planner must not adopt learner-only group {group:?}"
                );
                let with = plan_node_group_rebalance(learner, &groups, &live, &[], 3, 1);
                assert!(
                    with.adopt.contains(group),
                    "learner-aware planner should adopt group {group:?}"
                );
                let retire = plan_node_group_rebalance(learner, &groups, &live, &[*group], 3, 0);
                assert!(
                    retire.retire.contains(group),
                    "voter-only planner incorrectly retires hosted learner group"
                );
            }
        }
        assert!(
            found_learner_only,
            "fixture must include at least one learner-only assignment"
        );
    }

    #[test]
    fn node_should_host_group_covers_voters_and_learners() {
        use craft_proto::NodeId;

        let live = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let group = RaftGroupId(7);
        let voter = group_voters(group, &live, 3)[0];
        let learner = group_learners(group, &live, 3, 1)
            .into_iter()
            .find(|n| !group_voters(group, &live, 3).contains(n))
            .expect("learner exists");
        assert!(node_should_host_group(group, voter, &live, 3, 1));
        assert!(node_should_host_group(group, learner, &live, 3, 1));
        assert!(!node_should_host_group(group, NodeId(99), &live, 3, 1));
    }

    #[test]
    fn shard_count_expansion_plans_new_shard_range() {
        let plan = plan_shard_count_expansion(256, 512).expect("expand");
        assert_eq!(plan.from, 256);
        assert_eq!(plan.to, 512);
        assert_eq!(plan.new_shard_ids.len(), 256);
        assert_eq!(plan.new_shard_ids[0], ShardId(256));
    }

    #[test]
    fn shard_router_expand_updates_count() {
        let mut router = ShardRouter::new(64);
        let plan = router.expand_shard_count(128).expect("expand");
        assert_eq!(plan.to, 128);
        assert_eq!(router.shard_count(), 128);
    }

    #[test]
    fn stable_activation_does_not_remap_routable_keys() {
        let samples: Vec<Vec<u8>> = (0..200u16).map(|n| n.to_le_bytes().to_vec()).collect();
        let sample_refs: Vec<&[u8]> = samples.iter().map(std::vec::Vec::as_slice).collect();

        // Tier 1 modulus router remaps most keys when count doubles.
        let mut tier1 = ShardRouter::new(256);
        let before: Vec<_> = sample_refs.iter().map(|k| tier1.shard_for(k)).collect();
        tier1.expand_shard_count(512).expect("expand");
        let remapped = before
            .iter()
            .zip(sample_refs.iter())
            .filter(|(old, key)| **old != tier1.shard_for(key))
            .count();
        assert!(
            remapped > sample_refs.len() / 4,
            "modulus expansion should remap a large fraction"
        );

        // Stable router keeps virtual shard ids for already-routable keys.
        assert!(stable_router_preserves_routable_keys(
            256,
            512,
            &sample_refs
        ));
    }

    #[test]
    fn catalog_validation_requires_contiguous_ids() {
        assert!(validate_catalog(&[]).is_err());
        assert!(validate_catalog(&[RaftGroupId(1)]).is_err());
        assert!(validate_catalog(&[RaftGroupId(0), RaftGroupId(2)]).is_err());
        assert!(validate_catalog(&[RaftGroupId(0), RaftGroupId(0)]).is_err());
        assert!(validate_catalog(&[RaftGroupId(0), RaftGroupId(1)]).is_ok());
    }

    #[test]
    fn catalog_expansion_appends_contiguous_groups() {
        let catalog: Vec<_> = (0..3).map(RaftGroupId).collect();
        let plan = plan_catalog_expansion(&catalog, 2).expect("expand");
        assert_eq!(plan.from_len, 3);
        assert_eq!(plan.to_len, 5);
        assert_eq!(plan.new_groups, vec![RaftGroupId(3), RaftGroupId(4)]);
        let mut expanded = catalog;
        expanded.extend(plan.new_groups);
        assert!(validate_catalog(&expanded).is_ok());
    }

    #[test]
    fn catalog_expansion_moves_minimal_shard_fraction() {
        let before: Vec<_> = (0..4).map(RaftGroupId).collect();
        let plan = plan_catalog_expansion(&before, 1).expect("expand");
        let mut after = before.clone();
        after.extend(plan.new_groups);
        let before_map = shard_assignment(400, &before);
        let after_map = shard_assignment(400, &after);
        let mut moved = 0;
        for (shard, old) in &before_map {
            let new = after_map[shard];
            if new != *old {
                assert_eq!(new, RaftGroupId(4));
                moved += 1;
            }
        }
        assert!(moved > 0);
        assert!(moved < 200);
    }
}
