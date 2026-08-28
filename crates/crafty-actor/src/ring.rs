//! Consistent hash ring for actor instance selection ([cluster-routing]).
//!
//! Replaces naive `hash % N` so adding or removing an instance remaps only keys
//! near the affected vnode arc on the ring, not the entire keyspace.
//!
//! [client-and-routing#cluster-actor-routing]: ../../../docs/decisions/client-and-routing.md#cluster-actor-routing

use std::hash::{Hash, Hasher};

/// Virtual nodes placed on the ring per physical member (even spread).
pub const VIRTUAL_NODES: u32 = 64;

/// Stable FNV-1a hash of `bytes` (independent of `Hash` random state).
#[must_use]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// Hash a key with the standard library hasher (for `Hash` types).
#[must_use]
pub fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Salt derived from a group name so unrelated pools do not share ring layout.
#[must_use]
pub fn group_salt(group: &str) -> u64 {
    hash_bytes(group.as_bytes())
}

/// Pick the member index in `[0, member_count)` that owns `key_hash` on the ring.
#[must_use]
pub fn pick_index(key_hash: u64, member_count: usize, salt: u64) -> usize {
    if member_count <= 1 {
        return 0;
    }
    let mut best_member = 0usize;
    let mut best_dist = u64::MAX;
    for member in 0..member_count {
        for vnode in 0..VIRTUAL_NODES {
            let pos = vnode_position(member, vnode, salt);
            let dist = ring_distance(key_hash, pos);
            if dist < best_dist || (dist == best_dist && member < best_member) {
                best_dist = dist;
                best_member = member;
            }
        }
    }
    best_member
}

fn vnode_position(member_index: usize, vnode: u32, salt: u64) -> u64 {
    mix64(salt ^ member_index as u64 ^ (u64::from(vnode).wrapping_mul(0x9E37_79B9_7F4A_7C15)))
}

fn ring_distance(key: u64, pos: u64) -> u64 {
    if pos >= key {
        pos - key
    } else {
        u64::MAX - key + pos
    }
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_is_stable_for_a_key() {
        let salt = group_salt("workers");
        let a = pick_index(hash_bytes(b"order-42"), 4, salt);
        for _ in 0..20 {
            assert_eq!(pick_index(hash_bytes(b"order-42"), 4, salt), a);
        }
    }

    #[test]
    fn adding_a_member_moves_a_fraction_of_keys() {
        let salt = group_salt("w");
        let before: Vec<_> = (0..400)
            .map(|i| pick_index(hash_bytes(format!("k-{i}").as_bytes()), 3, salt))
            .collect();
        let after: Vec<_> = (0..400)
            .map(|i| pick_index(hash_bytes(format!("k-{i}").as_bytes()), 4, salt))
            .collect();
        let moved = before.iter().zip(&after).filter(|(b, a)| b != a).count();
        assert!(moved > 0, "adding a member should claim some keys");
        assert!(
            moved < 300,
            "ring should remap less than 3/4 of keys when 3→4 members (moved {moved}/400)"
        );
    }

    #[test]
    fn empty_member_count_returns_zero() {
        assert_eq!(pick_index(1, 0, 0), 0);
    }
}
