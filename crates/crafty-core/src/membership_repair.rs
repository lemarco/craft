//! Pure planners for voter replacement when a committed voter is permanently
//! unreachable (cluster learner elasticity).

use std::collections::BTreeMap;

use crafty_proto::{LogIndex, NodeId};

/// Multiplier applied to the reachability silence window before a unreachable
/// voter is replaced by promoting a learner.
pub const DEFAULT_VOTER_REPLACEMENT_GRACE_MULTIPLIER: u64 = 6;

/// Logical ticks a voter must stay unreachable before replacement is attempted.
#[must_use]
pub fn voter_replacement_grace_ticks(reachability_window: u64) -> u64 {
    reachability_window.saturating_mul(DEFAULT_VOTER_REPLACEMENT_GRACE_MULTIPLIER)
}

/// Every node id currently reserved in cluster membership.
#[must_use]
pub fn occupied_node_ids(voters: &[NodeId], learners: &[NodeId]) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = voters.iter().chain(learners).copied().collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Pick the lowest-id learner whose replication progress is caught up to
/// `commit_index`.
#[must_use]
pub fn pick_promotion_candidate(
    learners: &[NodeId],
    match_index: &BTreeMap<NodeId, LogIndex>,
    commit_index: LogIndex,
) -> Option<NodeId> {
    let mut sorted = learners.to_vec();
    sorted.sort();
    sorted
        .into_iter()
        .find(|&id| match_index.get(&id).is_some_and(|&idx| idx >= commit_index))
}

/// Remove `dead_voter` and promote `promote` from learners to voters.
///
/// # Panics
/// If `dead_voter` is absent from `voters`, `promote` is absent from `learners`,
/// or the resulting voter set would be empty.
#[must_use]
pub fn plan_voter_replacement(
    dead_voter: NodeId,
    mut voters: Vec<NodeId>,
    mut learners: Vec<NodeId>,
    promote: NodeId,
) -> (Vec<NodeId>, Vec<NodeId>) {
    assert!(voters.contains(&dead_voter), "dead voter must be in voters");
    assert!(learners.contains(&promote), "promote must be a learner");
    voters.retain(|id| *id != dead_voter);
    learners.retain(|id| *id != promote);
    assert!(!voters.is_empty(), "replacement must not empty voters");
    voters.push(promote);
    voters.sort();
    voters.dedup();
    learners.sort();
    learners.dedup();
    (voters, learners)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_promotion_candidate_prefers_lowest_caught_up_id() {
        let learners = vec![NodeId(5), NodeId(4), NodeId(6)];
        let mut match_index = BTreeMap::new();
        match_index.insert(NodeId(4), LogIndex(10));
        match_index.insert(NodeId(5), LogIndex(10));
        match_index.insert(NodeId(6), LogIndex(9));
        assert_eq!(
            pick_promotion_candidate(&learners, &match_index, LogIndex(10)),
            Some(NodeId(4))
        );
    }

    #[test]
    fn plan_voter_replacement_swaps_roles() {
        let (voters, learners) = plan_voter_replacement(
            NodeId(2),
            vec![NodeId(1), NodeId(2), NodeId(3)],
            vec![NodeId(4), NodeId(5)],
            NodeId(4),
        );
        assert_eq!(voters, vec![NodeId(1), NodeId(3), NodeId(4)]);
        assert_eq!(learners, vec![NodeId(5)]);
    }
}
