use trembita_core::Role;
use trembita_dashboard::{EventBus, Metrics};
use trembita_proto::{LogIndex, NodeId, Term};
use trembita_runtime::NodeStatus;

use super::telemetry::MembershipTelemetry;

fn status(voters: &[u64], reachable: &[u64]) -> NodeStatus {
    NodeStatus {
        id: NodeId(1),
        role: Role::Leader,
        term: Term(1),
        leader: Some(NodeId(1)),
        commit_index: LogIndex(0),
        last_applied: LogIndex(0),
        voters: voters.iter().copied().map(NodeId).collect(),
        learners: vec![],
        reachable: reachable.iter().copied().map(NodeId).collect(),
        reachable_members: reachable.iter().copied().map(NodeId).collect(),
    }
}

#[test]
fn reachability_delta_flags_a_crashed_voter_without_membership_change() {
    let mut telemetry = MembershipTelemetry::new(NodeId(1), EventBus::new(16), Metrics::new());
    let _ = telemetry.record(&status(&[1, 2, 3], &[1, 2, 3]));

    let delta = telemetry.record(&status(&[1, 2, 3], &[1, 2]));

    assert!(!delta.membership_changed);
    assert!(delta.reachability_changed);
    assert_eq!(delta.unreachable, vec![NodeId(3)]);
    assert!(delta.departed.is_empty());
}

#[test]
fn reachability_delta_triggers_on_heal_without_membership_change() {
    let mut telemetry = MembershipTelemetry::new(NodeId(1), EventBus::new(16), Metrics::new());
    let _ = telemetry.record(&status(&[1, 2, 3], &[1, 2]));

    let delta = telemetry.record(&status(&[1, 2, 3], &[1, 2, 3]));

    assert!(!delta.membership_changed);
    assert!(delta.reachability_changed);
    assert!(delta.unreachable.is_empty());
}
