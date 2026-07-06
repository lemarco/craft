//! Durability contract for the core (backlog B4): [`RaftNode::take_persist`]
//! must report exactly the hard-state / log delta an outer runtime has to fsync
//! before acting on a step, and [`RaftNode::restore`] must rebuild an
//! equivalent node from that persisted state.

use craft_core::{Config, RaftNode, Role};
use craft_proto::{
    AppendEntries, EntryPayload, LogEntry, LogId, LogIndex, NodeId, RaftRpc, RaftRpcReply,
    RequestVoteReply, Round, Term,
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

fn entry(index: u64, term: u64, byte: u8) -> LogEntry {
    LogEntry {
        term: Term(term),
        index: LogIndex(index),
        payload: EntryPayload::Command(vec![byte]),
    }
}

/// Elect `n` (3-node cluster) as leader in term 1.
fn elect_leader_term1(n: &mut RaftNode) {
    n.campaign();
    let _ = n.take_outputs();
    n.receive_reply(
        NodeId(2),
        RaftRpcReply::RequestVote(RequestVoteReply {
            term: Term(1),
            vote_granted: true,
            pre_vote: false,
        }),
    );
    let _ = n.take_outputs();
    assert!(n.is_leader());
}

#[test]
fn fresh_node_has_nothing_to_persist() {
    let mut n = node(1, &[1, 2, 3]);
    assert!(
        n.take_persist().is_none(),
        "a brand-new node's state equals the persisted defaults"
    );
}

#[test]
fn election_persists_term_vote_and_the_noop_entry() {
    let mut n = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut n);

    let p = n.take_persist().expect("election mutated durable state");
    assert!(p.hard_state_dirty, "term/vote changed during the election");
    assert_eq!(p.term, n.current_term());
    assert_eq!(p.voted_for, Some(NodeId(1)), "node voted for itself");
    // The election no-op sits at index 1 and must be persisted.
    assert_eq!(p.truncate_from, Some(LogIndex(1)));
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].index, LogIndex(1));
    assert_eq!(p.entries[0].payload, EntryPayload::Noop);

    assert!(
        n.take_persist().is_none(),
        "the delta is consumed once taken"
    );
}

#[test]
fn plain_proposal_persists_only_the_new_entry() {
    let mut n = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut n);
    let _ = n.take_persist(); // drain election delta

    let idx = n.propose(vec![7]).unwrap();
    assert_eq!(idx, LogIndex(2));

    let p = n.take_persist().expect("the appended command is dirty");
    assert!(!p.hard_state_dirty, "a plain propose changes no term/vote");
    assert_eq!(p.truncate_from, Some(LogIndex(2)));
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].index, LogIndex(2));
    assert_eq!(p.entries[0].payload, EntryPayload::Command(vec![7]));
}

#[test]
fn follower_append_then_conflict_reports_the_truncation_point() {
    let mut n = node(2, &[1, 2, 3]);

    // Leader 1 (term 1) ships two entries onto a fresh follower.
    n.receive(
        NodeId(1),
        RaftRpc::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log: LogId::ZERO,
            entries: vec![entry(1, 1, 10), entry(2, 1, 20)],
            leader_commit: LogIndex(0),
            round: Round::ZERO,
        }),
    );
    let p = n
        .take_persist()
        .expect("follower learned a term and appended");
    assert!(p.hard_state_dirty, "adopted term 1 from 0");
    assert_eq!(p.truncate_from, Some(LogIndex(1)));
    assert_eq!(p.entries.len(), 2);
    assert_eq!(p.entries[1].index, LogIndex(2));

    // A term-2 leader overwrites index 2 with a different entry: the follower
    // must truncate at 2 and re-append.
    n.receive(
        NodeId(3),
        RaftRpc::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(3),
            prev_log: LogId::new(Term(1), LogIndex(1)),
            entries: vec![entry(2, 2, 99)],
            leader_commit: LogIndex(0),
            round: Round::ZERO,
        }),
    );
    let p = n
        .take_persist()
        .expect("conflict truncation + re-append is dirty");
    assert_eq!(p.truncate_from, Some(LogIndex(2)), "cut at the conflict");
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].term, Term(2));
    assert_eq!(p.entries[0].payload, EntryPayload::Command(vec![99]));
}

#[test]
fn restore_rebuilds_an_equivalent_node_from_persisted_state() {
    // Drive a source node and accumulate its durable deltas into a simulated
    // log, exactly as a storage adapter would.
    let mut src = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut src);
    src.propose(vec![7]).unwrap();
    src.propose(vec![8]).unwrap();
    let _ = src.take_outputs();

    let mut log: Vec<LogEntry> = Vec::new();
    while let Some(p) = src.take_persist() {
        if let Some(from) = p.truncate_from {
            log.retain(|e| e.index.0 < from.0);
        }
        log.extend(p.entries);
    }
    assert_eq!(log.len(), 3, "no-op + two commands");

    let term = src.current_term();
    let vote = src.voted_for();

    let mut restored = RaftNode::restore(
        NodeId(1),
        [NodeId(1), NodeId(2), NodeId(3)],
        cfg(),
        term,
        vote,
        log,
    );

    // Durable state is recovered verbatim; volatile state is reset.
    assert_eq!(restored.current_term(), term);
    assert_eq!(restored.voted_for(), vote);
    assert_eq!(restored.last_log_index(), src.last_log_index());
    assert_eq!(restored.term_at(LogIndex(2)), src.term_at(LogIndex(2)));
    assert_eq!(restored.role(), Role::Follower);
    assert_eq!(restored.commit_index(), LogIndex(0));
    assert_eq!(restored.last_applied(), LogIndex(0));

    // Freshly recovered state is already durable — nothing to re-persist.
    assert!(restored.take_persist().is_none());
}
