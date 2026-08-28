//! Non-voting learner replica semantics (asymmetric replication).

use craft_core::{Config, RaftNode, Role};
use craft_proto::{
    AppendEntriesReply, LogId, LogIndex, Membership, NodeId, RaftRpc, RaftRpcReply, RequestVote,
    RequestVoteReply, Round, Term,
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

fn membership(voters: &[u64], learners: &[u64]) -> Membership {
    Membership {
        voters: voters.iter().copied().map(NodeId).collect(),
        voters_outgoing: Vec::new(),
        learners: learners.iter().copied().map(NodeId).collect(),
    }
}

fn learner_node(id: u64, voters: &[u64]) -> RaftNode {
    RaftNode::with_membership(NodeId(id), membership(voters, &[id]), cfg())
}

fn voter_node(id: u64, voters: &[u64]) -> RaftNode {
    RaftNode::with_membership(NodeId(id), membership(voters, &[]), cfg())
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

fn elect_leader(leader: &mut RaftNode) {
    leader.campaign();
    let _ = leader.take_outputs();
    grant(leader, 2, 1);
    grant(leader, 3, 1);
    ack(leader, 2, 1);
    ack(leader, 3, 1);
    assert!(leader.is_leader());
}

#[test]
fn learner_does_not_start_election_on_timeout() {
    let mut n = learner_node(4, &[1, 2, 3]);
    for _ in 0..200 {
        n.tick();
    }
    assert_eq!(n.role(), Role::Follower);
    assert!(
        n.take_outputs()
            .iter()
            .all(|o| !matches!(o, craft_core::Output::Send(_, RaftRpc::RequestVote(_)))),
        "learners must not solicit votes"
    );
}

#[test]
fn learner_refuses_to_grant_votes() {
    let mut n = learner_node(4, &[1, 2, 3]);
    n.receive(
        NodeId(2),
        RaftRpc::RequestVote(RequestVote {
            term: Term(2),
            candidate_id: NodeId(2),
            last_log: LogId::new(Term(0), LogIndex(0)),
            pre_vote: false,
        }),
    );
    let outs = n.take_outputs();
    let reply = outs
        .iter()
        .find_map(|o| match o {
            craft_core::Output::Reply(_, RaftRpcReply::RequestVote(r)) => Some(r),
            _ => None,
        })
        .expect("vote reply");
    assert!(!reply.vote_granted);
}

#[test]
fn add_learner_does_not_shrink_quorum() {
    let mut leader = voter_node(1, &[1, 2, 3]);
    elect_leader(&mut leader);

    leader
        .propose_membership(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
        .unwrap();
    let _ = leader.take_outputs();
    assert_eq!(leader.commit_index(), LogIndex(1), "joint not committed yet");

    ack(&mut leader, 2, 1);
    assert_eq!(leader.commit_index(), LogIndex(2));
    ack(&mut leader, 3, 1);
    assert_eq!(leader.commit_index(), LogIndex(3));

    let committed = leader.committed_membership();
    assert_eq!(committed.learners, vec![NodeId(4)]);
    assert_eq!(committed.voters, vec![NodeId(1), NodeId(2), NodeId(3)]);

    // Learner ack alone must not advance commit.
    ack(&mut leader, 4, 1);
    assert_eq!(leader.commit_index(), LogIndex(3));
}

#[test]
fn leader_replicates_to_committed_learner() {
    let mut leader = voter_node(1, &[1, 2, 3]);
    elect_leader(&mut leader);

    leader
        .propose_membership(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
        .unwrap();
    ack(&mut leader, 2, 1);
    ack(&mut leader, 3, 1);
    ack(&mut leader, 2, 1);
    ack(&mut leader, 3, 1);
    assert_eq!(leader.committed_membership().learners, vec![NodeId(4)]);

    leader.propose(b"cmd".to_vec()).unwrap();
    let outs = leader.take_outputs();
    assert!(
        outs.iter().any(|o| matches!(
            o,
            craft_core::Output::Send(NodeId(4), RaftRpc::AppendEntries(_))
        )),
        "leader must replicate to committed learners: {outs:?}"
    );
}
