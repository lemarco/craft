//! Multi-Raft shard routing helpers for integration tests.

use std::collections::BTreeMap;

use craft_core::{RaftGroupId, ShardRouter, place_shard};

/// Find two distinct routing keys that land on different Raft groups.
#[must_use]
pub fn find_keys_for_two_groups(shard_count: u32, groups: &[RaftGroupId]) -> (Vec<u8>, Vec<u8>) {
    let router = ShardRouter::new(shard_count);
    let mut by_group: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    for i in 0..10_000u32 {
        let key = format!("route-{i}").into_bytes();
        let shard = router.shard_for(&key);
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
