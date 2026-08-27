//! Leader lease-read tests (read-consistency, follower/lease reads).
//!
//! A lease read lets the leader answer `query` locally — with no fresh quorum
//! round — while it holds a valid leadership lease. The lease is earned when a
//! quorum acks a heartbeat round and lasts `election_timeout_min / 2` logical
//! ticks; it must never outlive the leader's term or survive a step-down.

use craft_core::{Config, NotLeader, RaftNode};
use craft_proto::{
    AppendEntriesReply, LogIndex, NodeId, RaftRpcReply, RequestVoteReply, Round, Term,
};

fn cfg() -> Config {
    Config {
        election_timeout_min: 100,
        election_timeout_max: 100,
        heartbeat_interval: 5,
        seed: 1,
    }
}

fn node(id: u64, members: &[u64]) -> RaftNode {
    RaftNode::new(NodeId(id), members.iter().copied().map(NodeId), cfg())
}

fn grant(n: &mut RaftNode, from: u64, term: u64) {
    n.receive_reply(
        NodeId(from),
        RaftRpcReply::RequestVote(RequestVoteReply {
            term: Term(term),
            vote_granted: true,
            pre_vote: false,
        }),
    );
    let _ = n.take_outputs();
}

fn ack(n: &mut RaftNode, from: u64, term: u64, round: u64) {
    n.receive_reply(
        NodeId(from),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(term),
            success: true,
            conflict_index: None,
            conflict_term: None,
            round: Round(round),
        }),
    );
    let _ = n.take_outputs();
}

/// Elect node 1 leader in term 1 (with votes from `quorum_peers`) and commit
/// its no-op (index 1) once those peers replicate it — but do **not** ack any
/// further heartbeat round, so no lease is held yet.
fn leader_with_committed_noop(members: &[u64], quorum_peers: &[u64]) -> RaftNode {
    let mut n = node(1, members);
    n.campaign();
    let _ = n.take_outputs();
    for p in quorum_peers {
        grant(&mut n, *p, 1);
    }
    assert!(n.is_leader(), "quorum_peers must form a voting majority");
    for p in quorum_peers {
        ack(&mut n, *p, 1, 1000);
    }
    assert_eq!(n.commit_index(), LogIndex(1), "no-op committed");
    n
}

#[test]
fn lease_read_on_a_follower_is_rejected() {
    let n = node(1, &[1, 2, 3]);
    assert_eq!(
        n.lease_read().unwrap_err(),
        NotLeader { leader: None },
        "a follower cannot serve a lease read"
    );
    assert!(!n.lease_valid());
}

#[test]
fn a_quorum_ack_grants_a_lease_read_at_the_committed_index() {
    // `leader_with_committed_noop` acks round 1000, which extends the lease.
    let n = leader_with_committed_noop(&[1, 2, 3], &[2]);
    assert!(n.lease_valid(), "quorum ack should have granted the lease");
    assert_eq!(
        n.lease_read().unwrap(),
        Some(LogIndex(1)),
        "a leased read serves at the committed index with no round-trip"
    );
}

#[test]
fn no_lease_is_held_before_a_quorum_acks_this_term() {
    // Elect a leader but never ack a heartbeat round: the no-op is uncommitted
    // and no quorum has confirmed the lease.
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1);
    assert!(n.is_leader());

    assert!(!n.lease_valid(), "self alone is not a quorum of three");
    assert_eq!(
        n.lease_read().unwrap(),
        None,
        "without a quorum-confirmed lease, the caller must fall back to ReadIndex"
    );
}

#[test]
fn a_lease_expires_without_renewal() {
    let mut n = leader_with_committed_noop(&[1, 2, 3], &[2]);
    assert!(n.lease_valid());

    // Advance past the lease window (election_timeout_min / 2 = 50 ticks). The
    // periodic heartbeats fire but no peer acks them, so the lease is not
    // renewed and must lapse.
    for _ in 0..60 {
        n.tick();
        let _ = n.take_outputs();
    }
    assert!(!n.lease_valid(), "an un-renewed lease must expire");
    assert_eq!(
        n.lease_read().unwrap(),
        None,
        "an expired lease falls back to ReadIndex"
    );
    assert!(
        n.is_leader(),
        "expiry does not cost leadership, only the lease"
    );
}

#[test]
fn a_renewed_lease_stays_valid() {
    let mut n = leader_with_committed_noop(&[1, 2, 3], &[2]);

    // Tick to just before expiry, then have the peer ack the newest heartbeat
    // round: the lease extends and the leader keeps serving reads locally.
    for _ in 0..40 {
        n.tick();
        let _ = n.take_outputs();
    }
    ack(&mut n, 2, 1, 1000);
    for _ in 0..40 {
        n.tick();
        let _ = n.take_outputs();
    }
    assert!(
        n.lease_valid(),
        "a freshly acked round should renew the lease"
    );
    assert_eq!(n.lease_read().unwrap(), Some(LogIndex(1)));
}

#[test]
fn a_step_down_immediately_surrenders_the_lease() {
    let mut n = leader_with_committed_noop(&[1, 2, 3], &[2]);
    assert!(n.lease_valid());

    // A reply from a higher term forces a step-down mid-lease.
    n.receive_reply(
        NodeId(2),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(9),
            success: false,
            conflict_index: None,
            conflict_term: None,
            round: Round::ZERO,
        }),
    );
    let _ = n.take_outputs();

    assert!(!n.is_leader());
    assert!(!n.lease_valid(), "a deposed leader must not keep its lease");
    assert!(
        n.lease_read().is_err(),
        "a follower rejects lease reads outright"
    );
}

#[test]
fn a_single_node_leader_holds_a_lease_at_once() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();
    assert!(n.is_leader());
    assert_eq!(n.commit_index(), LogIndex(1));

    assert!(n.lease_valid(), "a quorum of one confirms its own lease");
    assert_eq!(n.lease_read().unwrap(), Some(LogIndex(1)));
}
