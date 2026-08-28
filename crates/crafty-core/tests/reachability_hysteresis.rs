//! Reachability hysteresis via [`RaftNode::reachable_now`] (liveness Tier 2).

use crafty_core::{Config, FailureDetectorKind, ReachabilityConfig, Role};
use crafty_proto::{AppendEntriesReply, NodeId, RaftRpcReply, RequestVoteReply, Round, Term};

fn cfg() -> Config {
    Config {
        election_timeout_min: 10,
        election_timeout_max: 20,
        heartbeat_interval: 2,
        seed: 1,
        reachability: ReachabilityConfig {
            window_ticks: Some(15),
            hysteresis_ticks: 5,
            detector: FailureDetectorKind::AckWindow,
            phi_threshold: 8.0,
        },
    }
}

fn leader() -> crafty_core::RaftNode {
    let mut n = crafty_core::RaftNode::new(NodeId(1), [NodeId(1), NodeId(2), NodeId(3)], cfg());
    n.campaign();
    let _ = n.take_outputs();
    for peer in [2_u64, 3] {
        n.receive_reply(
            NodeId(peer),
            RaftRpcReply::RequestVote(RequestVoteReply {
                term: Term(1),
                vote_granted: true,
                pre_vote: false,
            }),
        );
        let _ = n.take_outputs();
    }
    assert_eq!(n.role(), Role::Leader);
    n
}

fn ack(n: &mut crafty_core::RaftNode, from: u64, round: u64) {
    n.receive_reply(
        NodeId(from),
        RaftRpcReply::AppendEntries(AppendEntriesReply {
            term: Term(1),
            success: true,
            round: Round(round),
            conflict_index: None,
            conflict_term: None,
        }),
    );
    let _ = n.take_outputs();
}

#[test]
fn reachable_now_hysteresis_avoids_flap_on_brief_silence() {
    let mut n = leader();

    // Peer 2 acks at t=0.
    ack(&mut n, 2, 1);

    for _ in 0..12 {
        n.tick();
        let _ = n.take_outputs();
    }
    assert!(n.reachable_now().contains(&NodeId(2)));

    // Silence through the mark-unreachable threshold.
    for _ in 0..5 {
        n.tick();
        let _ = n.take_outputs();
    }
    assert!(!n.reachable_now().contains(&NodeId(2)));

    // Fresh ack — reachable again without waiting the full upper window.
    ack(&mut n, 2, 2);
    n.tick();
    let _ = n.take_outputs();
    assert!(n.reachable_now().contains(&NodeId(2)));
}
