//! Linearizable `ReadIndex` tests (read-consistency).
//!
//! A read must not be served until the leader confirms it still leads (a
//! quorum acks a heartbeat round issued after the request) and the state
//! machine has applied through the captured read index.

use crafty_core::{Config, NotLeader, Output, RaftNode, ReadId};
use crafty_proto::{
    AppendEntriesReply, LogIndex, NodeId, RaftRpcReply, RequestVoteReply, Round, Term,
};

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

/// Ack a heartbeat/replication as `from` at heartbeat `round`.
fn ack(n: &mut RaftNode, from: u64, term: u64, round: u64) -> Vec<Output> {
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
    n.take_outputs()
}

fn read_ready(outs: &[Output], id: ReadId) -> Option<LogIndex> {
    outs.iter().find_map(|o| match o {
        Output::ReadReady { id: rid, index } if *rid == id => Some(*index),
        _ => None,
    })
}

fn read_failed(outs: &[Output], id: ReadId) -> bool {
    outs.iter()
        .any(|o| matches!(o, Output::ReadFailed { id: rid } if *rid == id))
}

/// Elect node 1 leader in term 1 (with votes from `quorum_peers`) and commit
/// its no-op (index 1) once those peers replicate it.
fn leader_with_committed_noop(members: &[u64], quorum_peers: &[u64]) -> RaftNode {
    let mut n = node(1, members);
    n.campaign();
    let _ = n.take_outputs();
    for p in quorum_peers {
        grant(&mut n, *p, 1);
    }
    assert!(n.is_leader(), "quorum_peers must form a voting majority");
    // A large round satisfies any read registered later in the test.
    for p in quorum_peers {
        let _ = ack(&mut n, *p, 1, 1000);
    }
    assert_eq!(n.commit_index(), LogIndex(1), "no-op committed");
    n
}

#[test]
fn read_on_follower_is_rejected() {
    let mut n = node(1, &[1, 2, 3]);
    assert_eq!(
        n.read_index(ReadId(1)).unwrap_err(),
        NotLeader { leader: None }
    );
}

#[test]
fn read_completes_only_after_quorum_confirmation() {
    let mut n = leader_with_committed_noop(&[1, 2, 3], &[2]);

    n.read_index(ReadId(7)).unwrap();
    let outs = n.take_outputs();
    assert!(
        read_ready(&outs, ReadId(7)).is_none(),
        "self alone is not a quorum of three"
    );

    // A peer acks the heartbeat round -> quorum confirmed -> read is ready.
    let outs = ack(&mut n, 2, 1, 1000);
    assert_eq!(
        read_ready(&outs, ReadId(7)),
        Some(LogIndex(1)),
        "read serves at the committed index"
    );
}

#[test]
fn read_needs_a_full_quorum_in_a_five_node_cluster() {
    let mut n = leader_with_committed_noop(&[1, 2, 3, 4, 5], &[2, 3]);

    n.read_index(ReadId(1)).unwrap();
    let _ = n.take_outputs();

    // self + 1 peer = 2 of 5: below quorum.
    let outs = ack(&mut n, 2, 1, 1000);
    assert!(read_ready(&outs, ReadId(1)).is_none());

    // self + 2 peers = 3 of 5: quorum reached.
    let outs = ack(&mut n, 3, 1, 1000);
    assert_eq!(read_ready(&outs, ReadId(1)), Some(LogIndex(1)));
}

#[test]
fn stale_round_acks_do_not_confirm_a_later_read() {
    let mut n = leader_with_committed_noop(&[1, 2, 3], &[2]);

    // Register a read; its confirmation round is strictly positive.
    n.read_index(ReadId(9)).unwrap();
    let _ = n.take_outputs();

    // An ack echoing round 0 predates the read and must not confirm it.
    let outs = ack(&mut n, 2, 1, 0);
    assert!(
        read_ready(&outs, ReadId(9)).is_none(),
        "a stale-round ack cannot confirm leadership for this read"
    );
}

#[test]
fn read_fails_when_leadership_is_lost() {
    let mut n = leader_with_committed_noop(&[1, 2, 3], &[2]);

    n.read_index(ReadId(3)).unwrap();
    let _ = n.take_outputs();

    // A reply from a higher term forces a step-down; the pending read fails.
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
    let outs = n.take_outputs();
    assert!(read_failed(&outs, ReadId(3)));
    assert!(!n.is_leader());
}

#[test]
fn single_node_read_completes_immediately() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();
    assert!(n.is_leader());
    assert_eq!(n.commit_index(), LogIndex(1));

    n.read_index(ReadId(1)).unwrap();
    let outs = n.take_outputs();
    assert_eq!(
        read_ready(&outs, ReadId(1)),
        Some(LogIndex(1)),
        "a quorum of one confirms itself"
    );
}
