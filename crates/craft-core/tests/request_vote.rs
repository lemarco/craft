//! RequestVote receiver-rule tests (Raft §5.2, §5.4.1) with edge cases.

use craft_core::{Config, RaftNode};
use craft_proto::{
    AppendEntries, EntryPayload, LogEntry, LogIndex, NodeId, RaftRpc, RaftRpcReply, RequestVote,
    RequestVoteReply, Term,
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

fn entry(term: u64, index: u64) -> LogEntry {
    LogEntry {
        term: Term(term),
        index: LogIndex(index),
        payload: EntryPayload::Command(vec![index as u8]),
    }
}

/// Give a fresh follower a log at `term` by replaying a leader's entries.
fn install_log(n: &mut RaftNode, leader: u64, term: u64, entries: Vec<LogEntry>) {
    let ae = AppendEntries {
        term: Term(term),
        leader_id: NodeId(leader),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        entries,
        leader_commit: LogIndex(0),
    };
    n.receive(NodeId(leader), RaftRpc::AppendEntries(ae));
    let _ = n.take_outputs();
}

fn send_vote(
    n: &mut RaftNode,
    from: u64,
    term: u64,
    last_term: u64,
    last_index: u64,
) -> RequestVoteReply {
    let rv = RequestVote {
        term: Term(term),
        candidate_id: NodeId(from),
        last_log_index: LogIndex(last_index),
        last_log_term: Term(last_term),
        pre_vote: false,
    };
    n.receive(NodeId(from), RaftRpc::RequestVote(rv));
    n.take_outputs()
        .into_iter()
        .find_map(|o| match o {
            craft_core::Output::Reply(_, RaftRpcReply::RequestVote(r)) => Some(r),
            _ => None,
        })
        .expect("expected a RequestVote reply")
}

#[test]
fn grants_vote_to_up_to_date_candidate() {
    let mut n = node(1, &[1, 2, 3]);
    let reply = send_vote(&mut n, 2, 1, 0, 0);
    assert!(reply.vote_granted);
    assert_eq!(reply.term, Term(1));
    assert_eq!(n.voted_for(), Some(NodeId(2)));
    assert_eq!(n.current_term(), Term(1));
}

#[test]
fn denies_vote_when_candidate_term_is_stale() {
    let mut n = node(1, &[1, 2, 3]);
    install_log(&mut n, 9, 5, vec![entry(5, 1)]);
    let reply = send_vote(&mut n, 2, 3, 5, 1);
    assert!(!reply.vote_granted);
    assert_eq!(reply.term, Term(5), "reply carries our higher term");
    assert_eq!(n.voted_for(), None);
}

#[test]
fn steps_down_and_grants_on_higher_term() {
    let mut n = node(1, &[1, 2, 3]);
    install_log(&mut n, 9, 2, vec![entry(2, 1)]);
    // Higher term than ours, and candidate's log is at least as up to date.
    let reply = send_vote(&mut n, 3, 5, 2, 1);
    assert!(reply.vote_granted);
    assert_eq!(n.current_term(), Term(5));
    assert_eq!(n.voted_for(), Some(NodeId(3)));
}

#[test]
fn denies_second_candidate_in_same_term() {
    let mut n = node(1, &[1, 2, 3]);
    let first = send_vote(&mut n, 2, 3, 0, 0);
    assert!(first.vote_granted);
    let second = send_vote(&mut n, 3, 3, 0, 0);
    assert!(!second.vote_granted, "already voted this term");
    assert_eq!(n.voted_for(), Some(NodeId(2)));
}

#[test]
fn grant_is_idempotent_for_same_candidate() {
    let mut n = node(1, &[1, 2, 3]);
    assert!(send_vote(&mut n, 2, 3, 0, 0).vote_granted);
    assert!(
        send_vote(&mut n, 2, 3, 0, 0).vote_granted,
        "same candidate re-asks"
    );
}

#[test]
fn denies_candidate_with_lower_last_term() {
    let mut n = node(1, &[1, 2, 3]);
    install_log(&mut n, 9, 2, vec![entry(1, 1), entry(2, 2)]);
    // Candidate has a longer log but an older last term -> less up to date.
    let reply = send_vote(&mut n, 3, 3, 1, 9);
    assert!(!reply.vote_granted);
}

#[test]
fn denies_candidate_with_shorter_log_same_last_term() {
    let mut n = node(1, &[1, 2, 3]);
    install_log(&mut n, 9, 2, vec![entry(1, 1), entry(2, 2)]);
    // Same last term (2) but shorter log (index 1 < our 2).
    let reply = send_vote(&mut n, 3, 3, 2, 1);
    assert!(!reply.vote_granted);
}

#[test]
fn grants_candidate_with_equal_log() {
    let mut n = node(1, &[1, 2, 3]);
    install_log(&mut n, 9, 2, vec![entry(1, 1), entry(2, 2)]);
    let reply = send_vote(&mut n, 3, 3, 2, 2);
    assert!(reply.vote_granted, "equal logs count as up to date");
}
