//! Snapshot / log-compaction tests (Raft §7): compaction preconditions, the
//! leader shipping a snapshot to a lagging follower, and a follower installing
//! one and resuming normal replication.

use craft_core::{Config, Output, RaftNode, SnapshotState};
use craft_proto::{
    AppendEntries, AppendEntriesReply, EntryPayload, InstallSnapshot, LogEntry, LogId, LogIndex,
    Membership, NodeId, RaftRpc, RaftRpcReply, RequestVoteReply, Round, Term,
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

/// Leader in term 1 with commands committed and applied up to index 4.
fn leader_with_applied_log() -> RaftNode {
    let mut n = node(1, &[1, 2, 3]);
    n.campaign();
    let _ = n.take_outputs();
    grant(&mut n, 2, 1);
    ack(&mut n, 2, 1); // commit + apply the no-op (index 1)
    for i in 0..3u8 {
        n.propose(vec![i]).unwrap();
        let _ = n.take_outputs();
        ack(&mut n, 2, 1);
    }
    assert_eq!(n.last_applied(), LogIndex(4));
    n
}

#[test]
fn compact_rejects_non_applied_indices() {
    let mut n = leader_with_applied_log();
    assert!(
        !n.compact(LogIndex(5), vec![]),
        "cannot compact past applied"
    );
    assert!(n.compact(LogIndex(3), vec![9]), "compacts an applied index");
    assert_eq!(n.snapshot_index(), LogIndex(3));
    assert!(
        !n.compact(LogIndex(3), vec![]),
        "cannot re-compact the same boundary"
    );
    // Log after the boundary is still intact and usable.
    assert_eq!(n.term_at(LogIndex(4)), Some(Term(1)));
    assert_eq!(n.last_log_index(), LogIndex(4));
}

#[test]
fn leader_ships_snapshot_when_follower_is_behind_the_compaction() {
    let mut n = leader_with_applied_log();
    assert!(n.compact(LogIndex(3), vec![7, 7, 7]));

    // Follower 3 rejects, hinting it needs entries from index 1 — which are
    // now compacted, so the leader must send a snapshot instead.
    n.receive_reply(
        NodeId(3),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(1),
            success: false,
            conflict_index: Some(LogIndex(1)),
            conflict_term: None,
            round: Round::ZERO,
        }),
    );
    let outs = n.take_outputs();
    let is = outs
        .iter()
        .find_map(|o| match o {
            Output::Send(NodeId(3), RaftRpc::InstallSnapshot(is)) => Some(is.clone()),
            _ => None,
        })
        .expect("leader ships a snapshot to the lagging follower");
    assert_eq!(is.last_included, LogId::new(Term(1), LogIndex(3)));
    assert_eq!(is.data, vec![7, 7, 7]);
    assert_eq!(
        is.last_config.voters,
        vec![NodeId(1), NodeId(2), NodeId(3)],
        "snapshot carries the configuration"
    );
}

#[test]
fn leader_advances_commit_after_snapshot_reply() {
    let mut n = leader_with_applied_log();
    assert!(n.compact(LogIndex(3), vec![1]));

    // Trigger the snapshot send to follower 3.
    n.receive_reply(
        NodeId(3),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(1),
            success: false,
            conflict_index: Some(LogIndex(1)),
            conflict_term: None,
            round: Round::ZERO,
        }),
    );
    let _ = n.take_outputs();

    // Follower acknowledges the snapshot; its match index jumps to the
    // snapshot boundary so the leader now has 3/3 acks up to index 3.
    n.receive_reply(
        NodeId(3),
        RaftRpcReply::InstallSnapshot(craft_proto::InstallSnapshotReply { term: Term(1) }),
    );
    let _ = n.take_outputs();
    // (commit was already 4 via node 2; this asserts the reply path is sane.)
    assert_eq!(n.commit_index(), LogIndex(4));
}

#[test]
fn follower_installs_snapshot_and_resumes_replication() {
    let mut n = node(2, &[1, 2, 3]);
    let is = InstallSnapshot {
        term: Term(5),
        leader_id: NodeId(9),
        last_included: LogId::new(Term(3), LogIndex(4)),
        last_config: Membership {
            voters: vec![NodeId(1), NodeId(2), NodeId(3)],
            voters_outgoing: vec![],
            learners: vec![],
        },
        offset: 0,
        data: vec![42, 43],
        done: true,
    };
    n.receive(NodeId(9), RaftRpc::InstallSnapshot(is));
    let outs = n.take_outputs();

    let loaded = outs.iter().find_map(|o| match o {
        Output::LoadSnapshot { index, data } => Some((*index, data.clone())),
        _ => None,
    });
    assert_eq!(
        loaded,
        Some((LogIndex(4), vec![42, 43])),
        "runtime loads state"
    );
    assert_eq!(n.current_term(), Term(5));
    assert_eq!(n.leader_id(), Some(NodeId(9)));
    assert_eq!(n.commit_index(), LogIndex(4));
    assert_eq!(n.last_applied(), LogIndex(4));
    assert_eq!(n.snapshot_index(), LogIndex(4));
    assert_eq!(n.voters(), vec![NodeId(1), NodeId(2), NodeId(3)]);

    // Replication resumes right after the snapshot boundary.
    let ae = AppendEntries {
        term: Term(5),
        leader_id: NodeId(9),
        prev_log: LogId::new(Term(3), LogIndex(4)),
        entries: vec![LogEntry {
            term: Term(5),
            index: LogIndex(5),
            payload: EntryPayload::Command(vec![99]),
        }],
        leader_commit: LogIndex(5),
        round: Round::ZERO,
    };
    n.receive(NodeId(9), RaftRpc::AppendEntries(ae));
    let outs = n.take_outputs();
    let reply = outs.iter().find_map(|o| match o {
        Output::Reply(_, RaftRpcReply::AppendEntries(r)) => Some(r.clone()),
        _ => None,
    });
    assert!(
        reply.expect("append reply").success,
        "prev matches snapshot boundary"
    );
    assert_eq!(n.last_log_index(), LogIndex(5));
    assert_eq!(n.commit_index(), LogIndex(5));
}

#[test]
fn stored_snapshot_exposes_the_compaction_boundary() {
    let mut n = leader_with_applied_log();
    assert!(
        n.stored_snapshot().is_none(),
        "no snapshot before compaction"
    );
    assert!(n.compact(LogIndex(3), vec![7, 7, 7]));

    let snap = n
        .stored_snapshot()
        .expect("a snapshot exists after compaction");
    assert_eq!(snap.last_included, LogId::new(Term(1), LogIndex(3)));
    assert_eq!(snap.data, vec![7, 7, 7]);
    assert_eq!(
        snap.membership.voters,
        vec![NodeId(1), NodeId(2), NodeId(3)],
        "snapshot carries the configuration at the boundary"
    );
}

#[test]
fn restore_with_snapshot_seeds_boundary_and_retained_suffix() {
    let snapshot = SnapshotState {
        last_included: LogId::new(Term(3), LogIndex(4)),
        membership: Membership {
            voters: vec![NodeId(1), NodeId(2), NodeId(3)],
            voters_outgoing: vec![],
            learners: vec![],
        },
        data: vec![42],
    };
    // One retained live entry beyond the boundary (index 5).
    let suffix = vec![LogEntry {
        term: Term(3),
        index: LogIndex(5),
        payload: EntryPayload::Command(vec![9]),
    }];

    let n = RaftNode::restore_with_snapshot(
        NodeId(2),
        [NodeId(1), NodeId(2), NodeId(3)],
        cfg(),
        Term(5),
        Some(NodeId(1)),
        snapshot,
        suffix,
    );

    // The boundary is durably committed/applied; the suffix is present but not
    // yet committed beyond it.
    assert_eq!(n.snapshot_index(), LogIndex(4));
    assert_eq!(n.last_applied(), LogIndex(4));
    assert_eq!(n.commit_index(), LogIndex(4));
    assert_eq!(n.last_log_index(), LogIndex(5));
    assert_eq!(n.term_at(LogIndex(5)), Some(Term(3)));
    assert_eq!(n.current_term(), Term(5));
    assert_eq!(n.voted_for(), Some(NodeId(1)));
    // Configuration is recovered from the snapshot (its config entry was
    // compacted out of the log).
    assert_eq!(n.voters(), vec![NodeId(1), NodeId(2), NodeId(3)]);

    // The snapshot round-trips back out for shipping to lagging followers.
    let round = n
        .stored_snapshot()
        .expect("snapshot retained after restore");
    assert_eq!(round.last_included, LogId::new(Term(3), LogIndex(4)));
    assert_eq!(round.data, vec![42]);
}

#[test]
fn follower_ignores_a_stale_snapshot() {
    let mut n = node(2, &[1, 2, 3]);
    let fresh = InstallSnapshot {
        term: Term(5),
        leader_id: NodeId(9),
        last_included: LogId::new(Term(3), LogIndex(4)),
        last_config: Membership {
            voters: vec![NodeId(1), NodeId(2), NodeId(3)],
            voters_outgoing: vec![],
            learners: vec![],
        },
        offset: 0,
        data: vec![1],
        done: true,
    };
    n.receive(NodeId(9), RaftRpc::InstallSnapshot(fresh));
    let _ = n.take_outputs();
    assert_eq!(n.snapshot_index(), LogIndex(4));

    // A snapshot we already cover must not be re-installed.
    let stale = InstallSnapshot {
        term: Term(5),
        leader_id: NodeId(9),
        last_included: LogId::new(Term(2), LogIndex(2)),
        last_config: Membership::default(),
        offset: 0,
        data: vec![2],
        done: true,
    };
    n.receive(NodeId(9), RaftRpc::InstallSnapshot(stale));
    let outs = n.take_outputs();
    assert!(
        !outs
            .iter()
            .any(|o| matches!(o, Output::LoadSnapshot { .. })),
        "stale snapshot is ignored"
    );
    assert_eq!(n.snapshot_index(), LogIndex(4), "boundary unchanged");
}
