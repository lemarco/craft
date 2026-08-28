//! Meta-Raft queue autoscale policy entries (group 0 metadata).

use craft_core::{CatalogProposeError, Config, Output, RaftNode};
use craft_proto::{
    AppendEntries, AutoscalePolicyWire, EntryPayload, LogEntry, LogId, LogIndex, NodeId,
    QueueAutoscalePolicyCommand, RaftRpc, Round, Term,
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

fn elect_leader_term1(n: &mut RaftNode) {
    n.campaign();
    let _ = n.take_outputs();
    n.receive_reply(
        NodeId(2),
        craft_proto::RaftRpcReply::RequestVote(craft_proto::RequestVoteReply {
            term: Term(1),
            vote_granted: true,
            pre_vote: false,
        }),
    );
    let _ = n.take_outputs();
    assert!(n.is_leader());
}

fn sample_command() -> QueueAutoscalePolicyCommand {
    QueueAutoscalePolicyCommand {
        stream: "jobs".into(),
        worker: Some(AutoscalePolicyWire {
            worker_group: "workers".into(),
            target_pending_per_worker: 10,
            min_workers: 1,
            max_workers: 3,
            cooldown_ms: 30_000,
            poll_interval_ms: 5_000,
        }),
        membership: None,
    }
}

fn policy_applied(outs: &[Output]) -> Vec<(LogIndex, QueueAutoscalePolicyCommand)> {
    outs.iter()
        .filter_map(|o| match o {
            Output::QueueAutoscalePolicyApplied { index, command } => {
                Some((*index, command.clone()))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn follower_propose_queue_autoscale_policy_is_rejected() {
    let mut n = node(1, &[1, 2, 3]);
    let err = n
        .propose_queue_autoscale_policy(sample_command())
        .unwrap_err();
    assert!(matches!(
        err,
        CatalogProposeError::NotLeader { leader: None }
    ));
}

#[test]
fn leader_propose_queue_autoscale_policy_emits_applied_not_user_sm() {
    let mut n = node(1, &[1]);
    n.campaign();
    let _ = n.take_outputs();

    let cmd = sample_command();
    let index = n
        .propose_queue_autoscale_policy(cmd.clone())
        .expect("leader propose");
    let outs = n.take_outputs();

    assert_eq!(policy_applied(&outs), vec![(index, cmd)]);
    assert!(
        !outs.iter().any(|o| matches!(o, Output::Apply(_))),
        "policy must not hit the user SM"
    );
}

#[test]
fn queue_autoscale_policy_replicates_and_applies_on_follower() {
    let mut leader = node(1, &[1, 2, 3]);
    elect_leader_term1(&mut leader);

    let cmd = sample_command();
    let index = leader
        .propose_queue_autoscale_policy(cmd.clone())
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
                    payload: EntryPayload::QueueAutoscalePolicy(cmd.clone()),
                },
            ],
            leader_commit: index,
            round: Round::ZERO,
        }),
    );
    let follower_outs = follower.take_outputs();
    assert_eq!(policy_applied(&follower_outs), vec![(index, cmd)]);
}
