//! Leader-side replication, commit-advancement, and backtracking tests.

use craft_core::{Config, NotLeader, Output, RaftNode};
use craft_proto::{
    AppendEntries, AppendEntriesReply, EntryPayload, LogEntry, LogId, LogIndex, NodeId, RaftRpc,
    RaftRpcReply, RequestVoteReply, Round, Term,
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
}

fn ack(n: &mut RaftNode, from: u64, term: u64) -> Vec<Output> {
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
    n.take_outputs()
}

fn applied(outs: &[Output]) -> Vec<(LogIndex, Vec<u8>)> {
    outs.iter()
        .filter_map(|o| match o {
            Output::Apply(c) => Some((c.index, c.command.clone())),
            _ => None,
        })
        .collect()
}

/// Elect `n` (3-node cluster) as leader in term 1.
fn elect_leader_term1(n: &mut RaftNode) {
    n.campaign();
    let _ = n.take_outputs();
    grant(n, 2, 1);
    let _ = n.take_outputs();
    assert!(n.is_leader());
}

#[test]
fn propose_on_follower_is_rejected() {
    let mut n = node(1, &[1, 2, 3]);
    let err = n.propose(vec![1]).unwrap_err();
    assert_eq!(err, NotLeader { leader: None });
}

#[test]
fn propose_redirects_to_known_leader() {
    let mut n = node(1, &[1, 2, 3]);
    let ae = AppendEntries {
        term: Term(2),
        leader_id: NodeId(2),
        prev_log: LogId::ZERO,
        entries: vec![],
        leader_commit: LogIndex(0),
        round: Round::ZERO,
    };
    n.receive(NodeId(2), RaftRpc::AppendEntries(ae));
    let _ = n.take_outputs();
    assert_eq!(
        n.propose(vec![1]).unwrap_err(),
        NotLeader {
            leader: Some(NodeId(2))
        }
    );
}

#[test]
fn leader_appends_noop_on_election() {
    let mut n = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut n);
    assert_eq!(n.last_log_index(), LogIndex(1));
    assert_eq!(
        n.term_at(LogIndex(1)),
        Some(Term(1)),
        "no-op is a term-1 entry"
    );
}

#[test]
fn commits_and_applies_after_majority_ack() {
    let mut n = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut n);
    let idx = n.propose(vec![42]).unwrap();
    assert_eq!(idx, LogIndex(2));
    let _ = n.take_outputs();

    let outs = ack(&mut n, 2, 1); // one peer + self = quorum of 3
    assert_eq!(n.commit_index(), LogIndex(2));
    assert_eq!(applied(&outs), vec![(LogIndex(2), vec![42])]);
}

#[test]
fn no_commit_without_quorum() {
    let mut n = node(1, &[1, 2, 3, 4, 5]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1);
    grant(&mut n, 3, 1); // 3 of 5 -> leader
    let _ = n.take_outputs();
    n.propose(vec![7]).unwrap();
    let _ = n.take_outputs();

    let outs = ack(&mut n, 2, 1); // self + 1 = 2 of 5, below quorum (3)
    assert_eq!(n.commit_index(), LogIndex(0));
    assert!(applied(&outs).is_empty());

    let outs = ack(&mut n, 3, 1); // self + 2 = 3 of 5 -> commit
    assert_eq!(n.commit_index(), LogIndex(2));
    assert_eq!(applied(&outs), vec![(LogIndex(2), vec![7])]);
}

#[test]
fn prior_term_entry_commits_only_via_current_term_entry() {
    // Figure-8 safety: a leader must not consider a prior-term entry committed
    // by replica count alone; it commits indirectly once a current-term entry
    // reaches quorum.
    let mut n = node(1, &[1, 2, 3]);
    // Acquire a term-1 entry as a follower first.
    let ae = AppendEntries {
        term: Term(1),
        leader_id: NodeId(2),
        prev_log: LogId::ZERO,
        entries: vec![LogEntry {
            term: Term(1),
            index: LogIndex(1),
            payload: EntryPayload::Command(vec![100]),
        }],
        leader_commit: LogIndex(0),
        round: Round::ZERO,
    };
    n.receive(NodeId(2), RaftRpc::AppendEntries(ae));
    let _ = n.take_outputs();
    assert_eq!(n.commit_index(), LogIndex(0));

    // Become leader in term 2 (appends a term-2 no-op at index 2).
    n.campaign(); // term 2
    let _ = n.take_outputs();
    grant(&mut n, 2, 2);
    let _ = n.take_outputs();
    assert!(n.is_leader());
    assert_eq!(n.current_term(), Term(2));
    assert_eq!(
        n.commit_index(),
        LogIndex(0),
        "prior-term entry not yet committed"
    );

    // Majority acks; the term-2 no-op reaching quorum also commits index 1.
    let outs = ack(&mut n, 2, 2);
    assert_eq!(n.commit_index(), LogIndex(2));
    assert_eq!(
        applied(&outs),
        vec![(LogIndex(1), vec![100])],
        "the prior-term command now applies (no-op at 2 is skipped)"
    );
}

#[test]
fn backtracks_on_rejection_using_conflict_hint() {
    let mut n = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut n);
    let _ = n.take_outputs();

    // Follower 2 rejects, hinting its log diverges at index 1.
    n.receive_reply(
        NodeId(2),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(1),
            success: false,
            conflict_index: Some(LogIndex(1)),
            conflict_term: None,
            round: Round::ZERO,
        }),
    );
    let outs = n.take_outputs();
    let retried = outs.iter().find_map(|o| match o {
        Output::Send(NodeId(2), RaftRpc::AppendEntries(ae)) => Some(ae.clone()),
        _ => None,
    });
    let ae = retried.expect("leader retries with a lower prev index");
    assert_eq!(ae.prev_log.index, LogIndex(0), "backed off to the start");
    assert_eq!(ae.entries.len(), 1, "now ships the no-op from index 1");
}

#[test]
fn heartbeat_carries_updated_commit_index() {
    let mut n = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut n);
    n.propose(vec![5]).unwrap();
    let _ = n.take_outputs();
    let _ = ack(&mut n, 2, 1);
    assert_eq!(n.commit_index(), LogIndex(2));

    // Next heartbeat should advertise the advanced commit index to followers.
    for _ in 0..cfg().heartbeat_interval {
        n.tick();
    }
    let outs = n.take_outputs();
    let hb = outs.iter().find_map(|o| match o {
        Output::Send(NodeId(3), RaftRpc::AppendEntries(ae)) => Some(ae.clone()),
        _ => None,
    });
    assert_eq!(hb.expect("heartbeat to peer 3").leader_commit, LogIndex(2));
}
