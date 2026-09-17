//! B-35 — join → catch-up → auto-hosts → LB pool readiness.

use trembita_dashboard::{JoinPhase, JoinStatusView};
use trembita_proto::{LogIndex, NodeId};
use trembita_runtime::NodeStatus;

/// Evaluate elastic join lifecycle for ops [`Readiness`](trembita_dashboard::Readiness).
#[must_use]
pub fn evaluate_join_pipeline(
    node_id: NodeId,
    status: &NodeStatus,
    local_worker_names: &[String],
) -> JoinStatusView {
    let committed_voter = status.voters.contains(&node_id);
    let committed_learner = status.learners.contains(&node_id);
    let log_caught_up =
        status.last_applied >= status.commit_index && status.commit_index >= LogIndex(0);
    let hosts_wired = !local_worker_names.is_empty();

    let (phase, reason) = if !committed_voter && !committed_learner {
        (
            JoinPhase::AwaitingMembership,
            Some("awaiting committed cluster membership".into()),
        )
    } else if !log_caught_up {
        (
            JoinPhase::CatchingUp,
            Some("replicating committed Raft log".into()),
        )
    } else if committed_voter {
        (JoinPhase::PoolReady, None)
    } else if !hosts_wired {
        (
            JoinPhase::AwaitingHosts,
            Some("awaiting supervisor auto-hosts on this node".into()),
        )
    } else {
        (JoinPhase::PoolReady, None)
    };

    JoinStatusView {
        node_id: node_id.0,
        phase,
        committed_voter,
        committed_learner,
        log_caught_up,
        hosts_wired,
        local_workers: local_worker_names.to_vec(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use trembita_core::Role;
    use trembita_proto::Term;

    use super::*;

    fn status(voters: &[u32], learners: &[u32], applied: u64, commit: u64) -> NodeStatus {
        NodeStatus {
            id: NodeId(4),
            role: Role::Follower,
            term: Term(1),
            leader: Some(NodeId(1)),
            commit_index: LogIndex(commit),
            last_applied: LogIndex(applied),
            voters: voters
                .iter()
                .copied()
                .map(|id| NodeId(u64::from(id)))
                .collect(),
            learners: learners
                .iter()
                .copied()
                .map(|id| NodeId(u64::from(id)))
                .collect(),
            reachable: voters
                .iter()
                .copied()
                .map(|id| NodeId(u64::from(id)))
                .collect(),
            reachable_members: voters
                .iter()
                .chain(learners.iter())
                .copied()
                .map(|id| NodeId(u64::from(id)))
                .collect(),
        }
    }

    /// B-35 — membership → catch-up → auto-hosts → pool-ready.
    #[test]
    fn b35_join_pipeline_scenarios_table() {
        struct Row {
            name: &'static str,
            node: u32,
            voters: &'static [u32],
            learners: &'static [u32],
            applied: u64,
            commit: u64,
            workers: &'static [&'static str],
            want: JoinPhase,
        }

        let rows = [
            Row {
                name: "not in committed set",
                node: 4,
                voters: &[1, 2, 3],
                learners: &[],
                applied: 0,
                commit: 0,
                workers: &[],
                want: JoinPhase::AwaitingMembership,
            },
            Row {
                name: "learner replicating log",
                node: 4,
                voters: &[1, 2, 3],
                learners: &[4],
                applied: 3,
                commit: 10,
                workers: &[],
                want: JoinPhase::CatchingUp,
            },
            Row {
                name: "voter caught up",
                node: 2,
                voters: &[1, 2, 3],
                learners: &[],
                applied: 10,
                commit: 10,
                workers: &[],
                want: JoinPhase::PoolReady,
            },
            Row {
                name: "learner caught up, no hosts",
                node: 4,
                voters: &[1, 2, 3],
                learners: &[4],
                applied: 5,
                commit: 5,
                workers: &[],
                want: JoinPhase::AwaitingHosts,
            },
            Row {
                name: "learner pool-ready after auto-hosts",
                node: 4,
                voters: &[1, 2, 3],
                learners: &[4],
                applied: 10,
                commit: 10,
                workers: &["cap#1"],
                want: JoinPhase::PoolReady,
            },
        ];

        for row in rows {
            let s = status(row.voters, row.learners, row.applied, row.commit);
            let workers: Vec<String> = row.workers.iter().map(|w| (*w).to_string()).collect();
            let view = evaluate_join_pipeline(NodeId(u64::from(row.node)), &s, &workers);
            assert_eq!(view.phase, row.want, "{}: phase", row.name);
        }
    }
}
