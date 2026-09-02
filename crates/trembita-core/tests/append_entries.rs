//! `AppendEntries` receiver-rule tests (Raft §5.3) with conflict/commit edges.

use trembita_core::{Config, Output, RaftNode, Role};
use trembita_proto::{
    AppendEntries, AppendEntriesReply, EntryPayload, LogEntry, LogId, LogIndex, NodeId, RaftRpc,
    RaftRpcReply, Round, Term,
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

fn cmd_entry(term: u64, index: u64, b: u8) -> LogEntry {
    LogEntry {
        term: Term(term),
        index: LogIndex(index),
        payload: EntryPayload::Command(vec![b]),
    }
}

struct Sent {
    reply: AppendEntriesReply,
    applied: Vec<(LogIndex, Vec<u8>)>,
}

fn append(
    n: &mut RaftNode,
    leader: u64,
    term: u64,
    prev_index: u64,
    prev_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
) -> Sent {
    let ae = AppendEntries {
        term: Term(term),
        leader_id: NodeId(leader),
        prev_log: LogId::new(Term(prev_term), LogIndex(prev_index)),
        entries,
        leader_commit: LogIndex(leader_commit),
        round: Round::ZERO,
    };
    n.receive(NodeId(leader), RaftRpc::AppendEntries(ae));
    let outs = n.take_outputs();
    let reply = outs
        .iter()
        .find_map(|o| match o {
            Output::Reply(_, RaftRpcReply::AppendEntries(r)) => Some(r.clone()),
            _ => None,
        })
        .expect("expected an AppendEntries reply");
    let applied = outs
        .iter()
        .filter_map(|o| match o {
            Output::Apply(c) => Some((c.index, c.command.clone())),
            _ => None,
        })
        .collect();
    Sent { reply, applied }
}

#[test]
fn rejects_when_leader_term_is_stale() {
    let mut n = node(1, &[1, 2, 3]);
    // Bump our term to 5 via a legitimate leader.
    append(&mut n, 2, 5, 0, 0, vec![], 0);
    // A stale leader at term 3 must be rejected.
    let s = append(&mut n, 4, 3, 0, 0, vec![], 0);
    assert!(!s.reply.success);
    assert_eq!(s.reply.term, Term(5));
}

#[test]
fn accepts_heartbeat_and_sets_leader() {
    let mut n = node(1, &[1, 2, 3]);
    let s = append(&mut n, 2, 1, 0, 0, vec![], 0);
    assert!(s.reply.success);
    assert_eq!(n.leader_id(), Some(NodeId(2)));
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.current_term(), Term(1));
}

#[test]
fn rejects_when_prev_index_beyond_log() {
    let mut n = node(1, &[1, 2, 3]);
    let s = append(&mut n, 2, 1, 5, 1, vec![], 0);
    assert!(!s.reply.success);
    assert_eq!(
        s.reply.conflict_index,
        Some(LogIndex(1)),
        "hint points just past our (empty) log"
    );
}

#[test]
fn rejects_on_prev_term_mismatch_with_hint() {
    let mut n = node(1, &[1, 2, 3]);
    // Log: [t1@1]
    append(&mut n, 2, 1, 0, 0, vec![cmd_entry(1, 1, 10)], 0);
    // Leader claims prev@1 has term 2 -> mismatch (ours is term 1).
    let s = append(&mut n, 2, 2, 1, 2, vec![], 0);
    assert!(!s.reply.success);
    assert_eq!(s.reply.conflict_term, Some(Term(1)));
    assert_eq!(s.reply.conflict_index, Some(LogIndex(1)));
}

#[test]
fn truncates_conflicting_suffix_then_appends() {
    let mut n = node(1, &[1, 2, 3]);
    // Log: [t1@1, t1@2, t1@3]
    append(
        &mut n,
        2,
        1,
        0,
        0,
        vec![cmd_entry(1, 1, 1), cmd_entry(1, 2, 2), cmd_entry(1, 3, 3)],
        0,
    );
    assert_eq!(n.last_log_index(), LogIndex(3));
    // New leader term 2 overwrites from index 2 with a term-2 entry.
    let s = append(&mut n, 4, 2, 1, 1, vec![cmd_entry(2, 2, 99)], 0);
    assert!(s.reply.success);
    assert_eq!(n.last_log_index(), LogIndex(2));
    assert_eq!(n.term_at(LogIndex(2)), Some(Term(2)));
    assert_eq!(n.term_at(LogIndex(3)), None, "conflicting suffix removed");
}

#[test]
fn reappending_identical_entries_is_idempotent() {
    let mut n = node(1, &[1, 2, 3]);
    let entries = vec![cmd_entry(1, 1, 1), cmd_entry(1, 2, 2)];
    append(&mut n, 2, 1, 0, 0, entries.clone(), 0);
    assert_eq!(n.last_log_index(), LogIndex(2));
    // Same message redelivered (e.g. retransmit) must not grow the log.
    let s = append(&mut n, 2, 1, 0, 0, entries, 0);
    assert!(s.reply.success);
    assert_eq!(n.last_log_index(), LogIndex(2));
}

#[test]
fn advances_commit_and_applies_in_order() {
    let mut n = node(1, &[1, 2, 3]);
    // Replicate three commands, commit up to index 2.
    let s = append(
        &mut n,
        2,
        1,
        0,
        0,
        vec![
            cmd_entry(1, 1, 11),
            cmd_entry(1, 2, 22),
            cmd_entry(1, 3, 33),
        ],
        2,
    );
    assert!(s.reply.success);
    assert_eq!(n.commit_index(), LogIndex(2));
    assert_eq!(n.last_applied(), LogIndex(2));
    assert_eq!(
        s.applied,
        vec![(LogIndex(1), vec![11]), (LogIndex(2), vec![22])],
        "applies committed commands in index order, not the uncommitted 3rd"
    );
}

#[test]
fn commit_never_exceeds_last_new_entry() {
    let mut n = node(1, &[1, 2, 3]);
    // Leader commit is far ahead, but we only have up to index 1.
    let s = append(&mut n, 2, 1, 0, 0, vec![cmd_entry(1, 1, 1)], 99);
    assert!(s.reply.success);
    assert_eq!(n.commit_index(), LogIndex(1), "clamped to last local entry");
}

#[test]
fn candidate_steps_down_on_valid_append() {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    assert_eq!(n.role(), Role::Candidate);
    // A leader emerges in the same term; candidate must revert to follower.
    let term = n.current_term().0;
    let s = append(&mut n, 2, term, 0, 0, vec![], 0);
    assert!(s.reply.success);
    assert_eq!(n.role(), Role::Follower);
    assert_eq!(n.leader_id(), Some(NodeId(2)));
}
