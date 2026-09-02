//! Saga journal metadata entries (Meta-Raft saga journal) — not applied to the user SM.

use crafty_core::{CatalogProposeError, Config, Output, RaftNode};
use crafty_proto::{
    AppendEntries, EntryPayload, LogEntry, LogId, LogIndex, NodeId, RaftRpc, RaftRpcReply,
    RequestVoteReply, Round, SagaJournalCommand, Term,
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

fn elect_leader_term1(n: &mut RaftNode) {
    n.campaign();
    let _ = n.take_outputs();
    grant(n, 2, 1);
    let _ = n.take_outputs();
    assert!(n.is_leader());
}

fn sample_command() -> SagaJournalCommand {
    SagaJournalCommand {
        saga_id: b"transfer-x".to_vec(),
        record: vec![9, 8, 7],
    }
}

fn saga_applied(outs: &[Output]) -> Vec<(LogIndex, SagaJournalCommand)> {
    outs.iter()
        .filter_map(|o| match o {
            Output::SagaJournalApplied { index, command } => Some((*index, command.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn follower_propose_saga_journal_is_rejected() {
    let mut n = node(1, &[1, 2, 3]);
    let err = n.propose_saga_journal(sample_command()).unwrap_err();
    assert!(matches!(
        err,
        CatalogProposeError::NotLeader { leader: None }
    ));
}

#[test]
fn leader_propose_saga_journal_emits_applied_not_user_sm() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();

    let cmd = sample_command();
    let index = n.propose_saga_journal(cmd.clone()).expect("leader propose");
    let outs = n.take_outputs();

    assert_eq!(saga_applied(&outs), vec![(index, cmd)]);
    assert!(
        !outs.iter().any(|o| matches!(o, Output::Apply(_))),
        "saga journal must not hit the user state machine"
    );
}

#[test]
fn saga_journal_replicates_and_applies_on_follower() {
    let mut leader = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut leader);

    let cmd = sample_command();
    let index = leader.propose_saga_journal(cmd.clone()).expect("propose");
    assert_eq!(index, LogIndex(2), "election noop is index 1");

    let mut follower = node(2, &[1, 2, 3]);
    follower.receive(
        NodeId(1),
        RaftRpc::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log: LogId::ZERO,
            entries: vec![
                LogEntry {
                    term: Term(1),
                    index: LogIndex(1),
                    payload: EntryPayload::Noop,
                },
                LogEntry {
                    term: Term(1),
                    index,
                    payload: EntryPayload::SagaJournal(cmd.clone()),
                },
            ],
            leader_commit: index,
            round: Round::ZERO,
        }),
    );
    let follower_outs = follower.take_outputs();
    assert_eq!(saga_applied(&follower_outs), vec![(index, cmd)]);
    assert!(
        !follower_outs.iter().any(|o| matches!(o, Output::Apply(_))),
        "follower must not apply saga journal to user SM"
    );
}
