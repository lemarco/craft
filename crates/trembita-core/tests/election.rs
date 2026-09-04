//! Election and role-transition tests (Raft §5.2) with edge cases.

use trembita_core::{Config, Output, RaftNode, Role};
use trembita_proto::{
    AppendEntries, LogId, LogIndex, NodeId, RaftRpc, RaftRpcReply, RequestVote, RequestVoteReply,
    Round, Term,
};

fn cfg() -> Config {
    Config {
        election_timeout_min: 100,
        election_timeout_max: 100,
        heartbeat_interval: 3,
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
}

fn deny(n: &mut RaftNode, from: u64, term: u64) {
    n.receive_reply(
        NodeId(from),
        RaftRpcReply::RequestVote(RequestVoteReply {
            term: Term(term),
            vote_granted: false,
            pre_vote: false,
        }),
    );
}

fn pre_grant(n: &mut RaftNode, from: u64, term: u64) {
    n.receive_reply(
        NodeId(from),
        RaftRpcReply::RequestVote(RequestVoteReply {
            term: Term(term),
            vote_granted: true,
            pre_vote: true,
        }),
    );
}

fn count_vote_requests(outs: &[Output]) -> usize {
    outs.iter()
        .filter(|o| matches!(o, Output::Send(_, RaftRpc::RequestVote(_))))
        .count()
}

fn heartbeat(n: &mut RaftNode, leader: u64, term: u64) {
    let ae = AppendEntries {
        term: Term(term),
        leader_id: NodeId(leader),
        prev_log: LogId::ZERO,
        entries: vec![],
        leader_commit: LogIndex(0),
        round: Round::ZERO,
    };
    n.receive(NodeId(leader), RaftRpc::AppendEntries(ae));
    let _ = n.take_outputs();
}

fn pre_vote_reply(
    n: &mut RaftNode,
    from: u64,
    term: u64,
    last_term: u64,
    last_index: u64,
) -> RequestVoteReply {
    let rv = RequestVote {
        term: Term(term),
        candidate_id: NodeId(from),
        last_log: LogId::new(Term(last_term), LogIndex(last_index)),
        pre_vote: true,
    };
    n.receive(NodeId(from), RaftRpc::RequestVote(rv));
    n.take_outputs()
        .into_iter()
        .find_map(|o| match o {
            Output::Reply(_, RaftRpcReply::RequestVote(r)) => Some(r),
            _ => None,
        })
        .expect("expected a RequestVote reply")
}

#[test]
fn starts_fresh_as_follower() {
    let n = node(1, &[1, 2, 3]);
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.current_term(), Term(0));
    assert_eq!(n.voted_for(), None);
    assert!(!n.is_leader());
}

#[test]
fn election_timeout_starts_prevote_then_real_election() {
    let mut n = node(1, &[1, 2, 3]);
    for _ in 0..99 {
        n.tick();
    }
    assert_eq!(n.role(), Role::Follower, "not yet at timeout");
    n.tick(); // 100th tick reaches the timeout -> pre-vote (no term bump)
    assert_eq!(n.role(), Role::PreCandidate);
    assert_eq!(n.current_term(), Term(0), "pre-vote does not bump the term");
    assert_eq!(n.voted_for(), None, "pre-vote does not record a vote");
    let outs = n.take_outputs();
    assert_eq!(count_vote_requests(&outs), 2, "pre-vote asks both peers");

    // A pre-vote majority promotes to a real Candidate in the next term.
    // (Pre-vote replies carry the responder's current term, still 0.)
    pre_grant(&mut n, 2, 0);
    assert_eq!(n.role(), Role::Candidate);
    assert_eq!(n.current_term(), Term(1));
    assert_eq!(n.voted_for(), Some(NodeId(1)), "now votes for self");
    assert_eq!(
        count_vote_requests(&n.take_outputs()),
        2,
        "real vote asks both peers"
    );
}

#[test]
fn prevote_does_not_promote_without_majority() {
    let mut n = node(1, &[1, 2, 3, 4, 5]);
    for _ in 0..100 {
        n.tick();
    }
    assert_eq!(n.role(), Role::PreCandidate);
    pre_grant(&mut n, 2, 0); // 2 of 5 pre-votes
    assert_eq!(
        n.role(),
        Role::PreCandidate,
        "still short of a pre-vote majority"
    );
    assert_eq!(n.current_term(), Term(0), "term untouched while pre-voting");
}

#[test]
fn single_node_cluster_elects_itself_immediately() {
    let mut n = node(1, &[1]);
    n.campaign();
    assert!(n.is_leader());
    assert_eq!(n.current_term(), Term(1));
    // The leader's no-op is committed instantly (quorum of one).
    assert_eq!(n.commit_index(), LogIndex(1));
    assert_eq!(n.last_applied(), LogIndex(1));
    let applied: Vec<_> = n
        .take_outputs()
        .into_iter()
        .filter(|o| matches!(o, Output::Apply(_)))
        .collect();
    assert!(applied.is_empty(), "no-op produces no Apply");
}

#[test]
fn wins_election_with_majority() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    assert_eq!(n.role(), Role::Candidate);
    grant(&mut n, 2, 1); // 2 of 3 votes -> quorum
    assert!(n.is_leader());
    let outs = n.take_outputs();
    let appends = outs
        .iter()
        .filter(|o| matches!(o, Output::Send(_, RaftRpc::AppendEntries(_))))
        .count();
    assert_eq!(appends, 2, "new leader heartbeats both peers");
}

#[test]
fn stays_candidate_without_majority() {
    let mut n = node(1, &[1, 2, 3, 4, 5]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1); // 2 of 5
    assert_eq!(n.role(), Role::Candidate);
    deny(&mut n, 3, 1);
    deny(&mut n, 4, 1);
    assert_eq!(n.role(), Role::Candidate, "denials do not elect");
}

#[test]
fn duplicate_grants_do_not_over_count() {
    let mut n = node(1, &[1, 2, 3, 4, 5]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1);
    grant(&mut n, 2, 1); // same voter twice
    assert_eq!(
        n.role(),
        Role::Candidate,
        "still only 2 distinct votes of 5"
    );
    grant(&mut n, 3, 1);
    assert!(n.is_leader(), "third distinct vote wins");
}

#[test]
fn higher_term_reply_forces_step_down() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    n.receive_reply(
        NodeId(2),
        RaftRpcReply::RequestVote(RequestVoteReply {
            term: Term(9),
            vote_granted: false,
            pre_vote: false,
        }),
    );
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.current_term(), Term(9));
}

#[test]
fn stale_vote_reply_is_ignored() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign(); // term 1
    let _ = n.take_outputs();
    n.campaign(); // term 2 (re-election); term-1 replies are now stale
    let _ = n.take_outputs();
    grant(&mut n, 2, 1); // stale term
    assert_eq!(n.role(), Role::Candidate, "stale-term grant ignored");
    grant(&mut n, 2, 2); // current term
    assert!(n.is_leader());
}

#[test]
fn new_leader_append_ends_candidacy() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    let term = n.current_term().0;
    let ae = AppendEntries {
        term: Term(term),
        leader_id: NodeId(2),
        prev_log: LogId::ZERO,
        entries: vec![],
        leader_commit: LogIndex(0),
        round: Round::ZERO,
    };
    n.receive(NodeId(2), RaftRpc::AppendEntries(ae));
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.leader_id(), Some(NodeId(2)));
}

#[test]
fn prevote_rejects_while_leader_contact_is_recent() {
    let mut voter = node(2, &[1, 2, 3]);
    heartbeat(&mut voter, 1, 1);
    for _ in 0..5 {
        voter.tick();
    }
    let reply = pre_vote_reply(&mut voter, 3, 2, 0, 0);
    assert!(
        !reply.vote_granted,
        "pre-vote must be rejected while leader contact is still fresh"
    );
}

#[test]
fn prevote_grants_after_stale_leader_contact_even_if_election_timer_just_reset() {
    let mut voter = node(2, &[1, 2, 3]);
    heartbeat(&mut voter, 1, 1);
    for _ in 0..100 {
        voter.tick();
    }
    assert_eq!(voter.role(), Role::PreCandidate, "timed out into pre-vote");
    let reply = pre_vote_reply(&mut voter, 3, 2, 0, 0);
    assert!(
        reply.vote_granted,
        "stale leader contact must not block pre-vote after election timeout"
    );
}

#[test]
fn two_survivors_elect_after_leader_loss_via_prevote() {
    let mut n2 = node(2, &[1, 2, 3]);
    let mut n3 = node(3, &[1, 2, 3]);
    heartbeat(&mut n2, 1, 1);
    heartbeat(&mut n3, 1, 1);

    for _ in 0..100 {
        n2.tick();
        n3.tick();
    }
    assert_eq!(n2.role(), Role::PreCandidate);
    assert_eq!(n3.role(), Role::PreCandidate);

    let r23 = pre_vote_reply(&mut n3, 2, 2, 0, 0);
    assert!(r23.vote_granted, "node 3 should grant node 2's pre-vote");
    let r32 = pre_vote_reply(&mut n2, 3, 2, 0, 0);
    assert!(r32.vote_granted, "node 2 should grant node 3's pre-vote");

    pre_grant(&mut n2, 3, 0);
    assert_eq!(
        n2.role(),
        Role::Candidate,
        "node 2 promotes on pre-vote quorum"
    );
    pre_grant(&mut n3, 2, 0);
    assert_eq!(
        n3.role(),
        Role::Candidate,
        "node 3 promotes on pre-vote quorum"
    );
}
