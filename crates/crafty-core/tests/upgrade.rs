//! Grant ordering and rolling completion for the reference upgrade machine.

use crafty_core::upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeMachine, UpgradePhase, UpgradeQuery, UpgradeResponse,
    plan_next_grant,
};
use crafty_core::{LogIndex, StateMachine};
use crafty_proto::NodeId;

fn manifest() -> ArtifactManifest {
    ArtifactManifest {
        app_version: "1.1.0".into(),
        url: "file:///tmp/app".into(),
        sha256_hex: "00".repeat(64),
        min_protocol: None,
    }
}

fn members() -> Vec<NodeId> {
    vec![NodeId(1), NodeId(2), NodeId(3)]
}

#[test]
fn rolling_grants_follow_leader_last_lowest_id() {
    let leader = NodeId(2);
    let mut sm = UpgradeMachine::default();
    sm.apply(LogIndex(1), &UpgradeCommand::SetDesired(manifest()))
        .expect("set desired");

    let order = [NodeId(1), NodeId(3), NodeId(2)];
    for (step, expected) in order.iter().enumerate() {
        let next = plan_next_grant(sm.state(), &members(), leader);
        assert_eq!(next, Some(*expected), "step {step}");
        sm.apply(
            LogIndex(u64::try_from(step + 2).unwrap()),
            &UpgradeCommand::Grant { node_id: *expected },
        )
        .expect("grant");
        sm.apply(
            LogIndex(u64::try_from(step + 5).unwrap()),
            &UpgradeCommand::Report {
                node_id: *expected,
                phase: UpgradePhase::Ready,
            },
        )
        .expect("ready");
    }

    let UpgradeResponse::View(view) = sm
        .query(&UpgradeQuery::View {
            members: members(),
        })
        .expect("view")
    else {
        panic!("expected view");
    };
    assert!(view.fleet_ready);
    assert_eq!(plan_next_grant(sm.state(), &members(), leader), None);
}

#[test]
fn abort_stops_further_grants() {
    let mut sm = UpgradeMachine::default();
    sm.apply(LogIndex(1), &UpgradeCommand::SetDesired(manifest()))
        .unwrap();
    sm.apply(
        LogIndex(2),
        &UpgradeCommand::Abort {
            reason: "bad checksum".into(),
        },
    )
    .unwrap();
    assert_eq!(
        plan_next_grant(sm.state(), &members(), NodeId(1)),
        None
    );
}
