//! Joint-consensus membership-change tests (Raft §6, membership-early).

use trembita_core::{Config, MembershipError, RaftNode};
use trembita_proto::{
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

fn ack(n: &mut RaftNode, from: u64, term: u64) {
    n.receive_reply(
        NodeId(from),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(term),
            success: true,
            conflict_index: None,
            conflict_term: None,
            round: Round::ZERO,
        }),
    );
    let _ = n.take_outputs();
}

fn ids(v: &[u64]) -> Vec<NodeId> {
    v.iter().copied().map(NodeId).collect()
}

#[test]
fn add_voter_via_joint_consensus() {
    // Single-node cluster grows to two nodes.
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();
    assert!(n.is_leader());
    assert_eq!(n.commit_index(), LogIndex(1)); // no-op committed by quorum-of-one

    let idx = n.propose_membership(ids(&[1, 2]), []).unwrap();
    assert_eq!(idx, LogIndex(2), "joint config appended after the no-op");
    assert!(n.is_joint());
    let _ = n.take_outputs();
    assert_eq!(n.commit_index(), LogIndex(1), "joint needs node 2 to ack");

    // Node 2 replicates the joint config -> it commits, leader appends C_new.
    ack(&mut n, 2, 1);
    assert_eq!(n.commit_index(), LogIndex(2));
    assert!(!n.is_joint(), "C_new (final config) now trails the log");

    // Node 2 replicates C_new -> it commits; membership change complete.
    ack(&mut n, 2, 1);
    assert_eq!(n.commit_index(), LogIndex(3));
    assert_eq!(n.voters(), ids(&[1, 2]));
}

#[test]
fn remove_voter_via_joint_consensus() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1); // leader term 1
    ack(&mut n, 2, 1); // commit the no-op
    assert!(n.is_leader());

    n.propose_membership(ids(&[1, 2]), []).unwrap(); // drop node 3
    assert!(n.is_joint());
    let _ = n.take_outputs();

    // Joint requires majorities of {1,2} (new) and {1,2,3} (old); node 2 ack
    // plus the leader satisfies both.
    ack(&mut n, 2, 1);
    ack(&mut n, 2, 1);
    assert_eq!(n.voters(), ids(&[1, 2]));
    assert!(!n.is_joint());
}

#[test]
fn rejects_change_while_one_is_in_progress() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();
    n.propose_membership(ids(&[1, 2]), []).unwrap();
    let _ = n.take_outputs();
    // Joint config is uncommitted -> a second change must be refused.
    assert_eq!(
        n.propose_membership(ids(&[1, 2, 3]), []),
        Err(MembershipError::InProgress)
    );
}

#[test]
fn rejects_membership_on_follower() {
    let mut n = node(1, &[1, 2, 3]);
    assert_eq!(
        n.propose_membership(ids(&[1, 2]), []),
        Err(MembershipError::NotLeader { leader: None })
    );
}

#[test]
fn rejects_empty_voter_set() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();
    assert_eq!(
        n.propose_membership(ids(&[]), []),
        Err(MembershipError::EmptyVoters)
    );
}

#[test]
fn leader_steps_down_after_removing_itself() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1);
    ack(&mut n, 2, 1);
    assert!(n.is_leader());

    // Remove the leader itself; new voters are {2, 3}.
    n.propose_membership(ids(&[2, 3]), []).unwrap();
    let _ = n.take_outputs();

    // Only nodes 2 and 3 count toward the new configuration's quorum.
    for _ in 0..4 {
        ack(&mut n, 2, 1);
        ack(&mut n, 3, 1);
    }
    assert!(
        !n.is_leader(),
        "leader steps down once C_new (without it) commits"
    );
    assert_eq!(n.voters(), ids(&[2, 3]));
}
