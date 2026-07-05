//! Cluster configuration as a value object.
//!
//! [`Configuration`] is the resolved, set-based view of a [`Membership`] that
//! owns the quorum arithmetic. Keeping this logic in one small, well-tested
//! value object (rather than scattered `usize` counts across the FSM) makes
//! joint consensus (ADR 016) hard to get subtly wrong: a joint configuration
//! requires a majority in *both* the incoming and outgoing voter sets.

use std::collections::BTreeSet;

use craft_proto::{Membership, NodeId};

/// A resolved cluster configuration with quorum semantics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Configuration {
    voters: BTreeSet<NodeId>,
    outgoing: BTreeSet<NodeId>,
    learners: BTreeSet<NodeId>,
}

impl Configuration {
    /// Resolve a wire [`Membership`] into set form.
    #[must_use]
    pub fn from_membership(m: &Membership) -> Self {
        Self {
            voters: m.voters.iter().copied().collect(),
            outgoing: m.voters_outgoing.iter().copied().collect(),
            learners: m.learners.iter().copied().collect(),
        }
    }

    /// Render back to a wire [`Membership`] with sorted, de-duplicated members.
    #[must_use]
    pub fn to_membership(&self) -> Membership {
        Membership {
            voters: self.voters.iter().copied().collect(),
            voters_outgoing: self.outgoing.iter().copied().collect(),
            learners: self.learners.iter().copied().collect(),
        }
    }

    /// Whether this is a joint (transitional) configuration (ADR 016).
    #[must_use]
    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }

    /// Whether `id` may vote (member of either the incoming or outgoing set).
    #[must_use]
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.outgoing.contains(&id)
    }

    /// The incoming voter set, sorted.
    #[must_use]
    pub fn voters(&self) -> Vec<NodeId> {
        self.voters.iter().copied().collect()
    }

    /// Every node referenced by the configuration (voters, outgoing, learners).
    #[must_use]
    pub fn members(&self) -> BTreeSet<NodeId> {
        self.voters
            .iter()
            .chain(self.outgoing.iter())
            .chain(self.learners.iter())
            .copied()
            .collect()
    }

    /// All members except `me` — everyone this node must replicate to.
    #[must_use]
    pub fn peers(&self, me: NodeId) -> Vec<NodeId> {
        let mut m = self.members();
        m.remove(&me);
        m.into_iter().collect()
    }

    /// Voting peers except `me` — the recipients of vote requests.
    #[must_use]
    pub fn voter_peers(&self, me: NodeId) -> Vec<NodeId> {
        self.voters
            .iter()
            .chain(self.outgoing.iter())
            .copied()
            .filter(|id| *id != me)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Whether `acked` forms a quorum: a majority of the incoming voters and,
    /// during a joint configuration, also a majority of the outgoing voters.
    #[must_use]
    pub fn has_quorum(&self, acked: &BTreeSet<NodeId>) -> bool {
        majority(&self.voters, acked) && majority(&self.outgoing, acked)
    }
}

/// A majority of `set` is present in `acked`. An empty `set` imposes no
/// constraint (used for the absent outgoing half of a stable configuration).
fn majority(set: &BTreeSet<NodeId>, acked: &BTreeSet<NodeId>) -> bool {
    if set.is_empty() {
        return true;
    }
    let need = set.len() / 2 + 1;
    acked.iter().filter(|id| set.contains(id)).count() >= need
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(v: &[u64]) -> BTreeSet<NodeId> {
        v.iter().copied().map(NodeId).collect()
    }

    fn membership(voters: &[u64], outgoing: &[u64], learners: &[u64]) -> Membership {
        Membership {
            voters: voters.iter().copied().map(NodeId).collect(),
            voters_outgoing: outgoing.iter().copied().map(NodeId).collect(),
            learners: learners.iter().copied().map(NodeId).collect(),
        }
    }

    #[test]
    fn stable_majority() {
        let c = Configuration::from_membership(&membership(&[1, 2, 3], &[], &[]));
        assert!(!c.is_joint());
        assert!(c.has_quorum(&ids(&[1, 2])));
        assert!(!c.has_quorum(&ids(&[1])));
    }

    #[test]
    fn joint_needs_both_halves() {
        // New = {1,2,3}, old = {1,4,5}. A quorum needs a majority of each.
        let c = Configuration::from_membership(&membership(&[1, 2, 3], &[1, 4, 5], &[]));
        assert!(c.is_joint());
        assert!(
            !c.has_quorum(&ids(&[1, 2])),
            "majority of new but not of old"
        );
        assert!(c.has_quorum(&ids(&[1, 2, 4])), "majority of both");
        assert!(
            !c.has_quorum(&ids(&[4, 5, 1])),
            "majority of old but not new"
        );
    }

    #[test]
    fn learners_do_not_vote_but_are_peers() {
        let c = Configuration::from_membership(&membership(&[1, 2, 3], &[], &[9]));
        assert!(!c.is_voter(NodeId(9)));
        assert!(c.peers(NodeId(1)).contains(&NodeId(9)));
        assert!(!c.voter_peers(NodeId(1)).contains(&NodeId(9)));
        // A learner's ack never contributes to a quorum.
        assert!(!c.has_quorum(&ids(&[1, 9])));
    }

    #[test]
    fn single_node_is_its_own_quorum() {
        let c = Configuration::from_membership(&membership(&[1], &[], &[]));
        assert!(c.has_quorum(&ids(&[1])));
        assert!(c.peers(NodeId(1)).is_empty());
    }

    #[test]
    fn roundtrips_through_membership() {
        let m = membership(&[3, 1, 2], &[], &[7]);
        let c = Configuration::from_membership(&m);
        let back = c.to_membership();
        assert_eq!(back.voters, vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(back.learners, vec![NodeId(7)]);
    }
}
