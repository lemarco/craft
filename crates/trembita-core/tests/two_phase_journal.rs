//! Durable 2PC metadata entries — not applied to the user state machine.

use trembita_core::{CatalogProposeError, Config, Output, RaftNode};
use trembita_proto::{
    AppendEntries, EntryPayload, LogEntry, LogId, LogIndex, NodeId, RaftRpc, RaftRpcReply,
    RequestVoteReply, Round, Term, TwoPhaseAbortCommand, TwoPhasePrepareCommand,
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

fn sample_prepare() -> TwoPhasePrepareCommand {
    TwoPhasePrepareCommand {
        tx_id: b"tx-1".to_vec(),
        route_key: b"shard-key".to_vec(),
        command: vec![1, 2, 3],
        prepared_at_ms: 1_000,
    }
}

fn prepare_applied(outs: &[Output]) -> Vec<(LogIndex, TwoPhasePrepareCommand)> {
    outs.iter()
        .filter_map(|o| match o {
            Output::TwoPhasePrepareApplied { index, command } => Some((*index, command.clone())),
            _ => None,
        })
        .collect()
}

fn abort_applied(outs: &[Output]) -> Vec<(LogIndex, TwoPhaseAbortCommand)> {
    outs.iter()
        .filter_map(|o| match o {
            Output::TwoPhaseAbortApplied { index, command } => Some((*index, command.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn follower_propose_two_phase_prepare_is_rejected() {
    let mut n = node(1, &[1, 2, 3]);
    let err = n.propose_two_phase_prepare(sample_prepare()).unwrap_err();
    assert!(matches!(
        err,
        CatalogProposeError::NotLeader { leader: None }
    ));
}

#[test]
fn leader_propose_two_phase_prepare_emits_applied_not_user_sm() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();

    let cmd = sample_prepare();
    let index = n
        .propose_two_phase_prepare(cmd.clone())
        .expect("leader propose");
    let outs = n.take_outputs();

    assert_eq!(prepare_applied(&outs), vec![(index, cmd)]);
    assert!(
        !outs.iter().any(|o| matches!(o, Output::Apply(_))),
        "durable 2PC prepare must not hit the user state machine"
    );
}

#[test]
fn two_phase_prepare_replicates_and_applies_on_follower() {
    let mut leader = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut leader);

    let cmd = sample_prepare();
    let index = leader
        .propose_two_phase_prepare(cmd.clone())
        .expect("propose");
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
                    payload: EntryPayload::TwoPhasePrepare(cmd.clone()),
                },
            ],
            leader_commit: index,
            round: Round::ZERO,
        }),
    );
    let follower_outs = follower.take_outputs();
    assert_eq!(prepare_applied(&follower_outs), vec![(index, cmd)]);
}

#[test]
fn leader_propose_two_phase_abort_emits_applied() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();

    let cmd = TwoPhaseAbortCommand {
        tx_id: b"tx-1".to_vec(),
        route_key: b"shard-key".to_vec(),
    };
    let index = n
        .propose_two_phase_abort(cmd.clone())
        .expect("leader propose");
    let outs = n.take_outputs();
    assert_eq!(abort_applied(&outs), vec![(index, cmd)]);
}
