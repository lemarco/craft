//! Multi-Raft shard routing helpers for integration tests.

use std::collections::BTreeMap;

use trembita_core::{RaftGroupId, ShardRouter, ShardRoutingKind, StableShardRouter, place_shard};

/// Find two distinct routing keys that land on different Raft groups.
///
/// Uses stable virtual routing by default (matches new cluster defaults).
#[must_use]
pub fn find_keys_for_two_groups(active_count: u32, groups: &[RaftGroupId]) -> (Vec<u8>, Vec<u8>) {
    find_keys_for_two_groups_with_routing(active_count, groups, ShardRoutingKind::StableVirtual)
}

/// Find two routing keys under modulus routing routing.
#[must_use]
pub fn find_keys_for_two_groups_modulus(
    active_count: u32,
    groups: &[RaftGroupId],
) -> (Vec<u8>, Vec<u8>) {
    find_keys_for_two_groups_with_routing(active_count, groups, ShardRoutingKind::Modulus)
}

/// Find two distinct routing keys for the given routing mode.
///
/// # Panics
/// If no keys routing to the first two `groups` entries are found within the search limit.
#[must_use]
pub fn find_keys_for_two_groups_with_routing(
    active_count: u32,
    groups: &[RaftGroupId],
    routing: ShardRoutingKind,
) -> (Vec<u8>, Vec<u8>) {
    let limit = match routing {
        ShardRoutingKind::Modulus => 10_000,
        ShardRoutingKind::StableVirtual => 50_000,
    };
    let mut by_group: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    for i in 0..limit {
        let key = format!("route-{i}").into_bytes();
        let shard = match routing {
            ShardRoutingKind::Modulus => Some(ShardRouter::new(active_count).shard_for(&key)),
            ShardRoutingKind::StableVirtual => StableShardRouter::new(active_count).shard_for(&key),
        };
        let Some(shard) = shard else {
            continue;
        };
        let Some(group) = place_shard(shard, groups) else {
            continue;
        };
        by_group.entry(group.0).or_insert(key);
        if by_group.len() >= 2 {
            break;
        }
    }
    (
        by_group.get(&groups[0].0).expect("key0").clone(),
        by_group.get(&groups[1].0).expect("key1").clone(),
    )
}
