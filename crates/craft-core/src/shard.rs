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

use std::collections::BTreeMap;

/// A partition of the keyspace. Fixed count per cluster; each shard is owned by
/// exactly one Raft group at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u32);

/// Identifies one of the cluster's independent Raft groups (multi-Raft).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RaftGroupId(pub u32);

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

/// The full shard → owning-group assignment for `shard_count` shards over
/// `groups`, using [`place_shard`]. Empty when `groups` is empty.
#[must_use]
pub fn shard_assignment(
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
}
