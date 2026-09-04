//! Leader-side liveness / reachability tests (liveness-vs-membership).
//!
//! `RaftNode::reachable` reports the voters a leader has heard from recently —
//! a signal distinct from committed membership. A voter that stops acking is
//! flagged unreachable after the detection window, even though it remains a
//! committed voter, which is what lets crash detection run without waiting for
//! a `ConfChange`. A non-leader has no first-hand ack data and conservatively
//! reports the full voter set.

use trembita_core::{Config, RaftNode};
use trembita_proto::{AppendEntriesReply, NodeId, RaftRpcReply, RequestVoteReply, Round, Term};

fn cfg() -> Config {
    Config {
        election_timeout_min: 100,
        election_timeout_max: 100,
        heartbeat_interval: 5,
        seed: 1,
        ..Default::default()
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

fn ids(nodes: &[u64]) -> Vec<NodeId> {
    nodes.iter().copied().map(NodeId).collect()
}

fn leader(members: &[u64], quorum_peers: &[u64]) -> RaftNode {
    let mut n = node(1, members);
    n.campaign();
    let _ = n.take_outputs();
    for p in quorum_peers {
        grant(&mut n, *p, 1);
    }
    assert!(n.is_leader(), "quorum_peers must form a voting majority");
    n
}

#[test]
fn a_follower_reports_every_voter_as_reachable() {
    // A follower has no ack data of its own, so it must not flag anyone down;
    // it defers crash detection to the leader.
    let mut n = node(1, &[1, 2, 3]);
    let mut got = n.reachable(50);
    got.sort();
    assert_eq!(got, ids(&[1, 2, 3]));

    // Ticking (no leadership) does not change the conservative view.
    for _ in 0..200 {
        n.tick();
        let _ = n.take_outputs();
    }
    let mut got = n.reachable(50);
    got.sort();
    assert_eq!(got, ids(&[1, 2, 3]));
}

#[test]
fn a_leader_counts_itself_and_recently_acking_voters() {
    let mut n = leader(&[1, 2, 3], &[2, 3]);
    // Both peers ack the current round: all three are reachable.
    ack(&mut n, 2, 1, 1000);
    ack(&mut n, 3, 1, 1000);
    let mut got = n.reachable(50);
    got.sort();
    assert_eq!(got, ids(&[1, 2, 3]), "leader + two fresh acks");
}

#[test]
fn a_silent_voter_falls_out_of_the_reachable_set() {
    let mut n = leader(&[1, 2, 3], &[2, 3]);
    ack(&mut n, 2, 1, 1000);
    ack(&mut n, 3, 1, 1000);

    // Node 2 keeps acking heartbeats; node 3 goes silent. After the window,
    // only node 3 should be considered down — node 3 is still a committed
    // voter, but not reachable.
    for _ in 0..80 {
        n.tick();
        // Drain the periodic heartbeat and keep node 2 alive by acking it.
        let _ = n.take_outputs();
        ack(&mut n, 2, 1, 1000);
    }

    assert_eq!(
        n.voters(),
        ids(&[1, 2, 3]),
        "membership is unchanged — node 3 is still a committed voter"
    );
    let mut got = n.reachable(50);
    got.sort();
    assert_eq!(
        got,
        ids(&[1, 2]),
        "the silent voter is dropped from the liveness set"
    );
}

#[test]
fn reachability_is_earned_fresh_each_term() {
    // A leader that has heard from a peer, then loses and regains leadership,
    // must not carry the stale ack across the term boundary.
    let mut n = leader(&[1, 2, 3], &[2, 3]);
    ack(&mut n, 2, 1, 1000);
    ack(&mut n, 3, 1, 1000);
    assert_eq!(n.reachable(50).len(), 3);

    // A higher-term reply deposes the leader.
    n.receive_reply(
        NodeId(2),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(5),
            success: false,
            conflict_index: None,
            conflict_term: None,
            round: Round::ZERO,
        }),
    );
    let _ = n.take_outputs();
    assert!(!n.is_leader());

    // Win a fresh term. Before any ack in the new term, only self is proven
    // reachable; the others are unknown until they ack again.
    n.campaign();
    let _ = n.take_outputs();
    let term = n.current_term().0;
    grant(&mut n, 2, term);
    grant(&mut n, 3, term);
    assert!(n.is_leader());
    assert_eq!(
        n.reachable(50),
        ids(&[1]),
        "no prior-term ack counts toward this term's liveness"
    );
    assert_eq!(
        n.reachable_now(),
        ids(&[1]),
        "reachable_now must not carry stale ack_liveness across terms"
    );
}

#[test]
fn reachable_now_drops_silent_voter_after_window() {
    let mut n = leader(&[1, 2, 3], &[2, 3]);
    ack(&mut n, 2, 1, 1000);
    ack(&mut n, 3, 1, 1000);
    for _ in 0..250 {
        n.tick();
        let _ = n.take_outputs();
        ack(&mut n, 2, 1, 1000);
    }
    let mut got = n.reachable_now();
    got.sort();
    assert_eq!(got, ids(&[1, 2]));
}

#[test]
fn a_single_node_leader_is_always_reachable() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();
    assert!(n.is_leader());
    assert_eq!(n.reachable(50), ids(&[1]));
    assert_eq!(n.reachable_now(), ids(&[1]));
}
