use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use trembita_proto::NodeId;
use trembita_runtime::{ClusterState, NodeStatus};

/// The live leadership/membership facts the supervisor reconciles against
/// (implements [`ClusterState`]), refreshed from the node's consensus status by
/// a background task. Exposed only so [`super::TrembitaCluster::supervisor`] has a nameable
/// type; you rarely construct or read it directly.
#[derive(Default)]
pub struct ClusterFacts {
    leader: AtomicBool,
    leader_id: Mutex<Option<NodeId>>,
    voters: Mutex<Vec<NodeId>>,
    learners: Mutex<Vec<NodeId>>,
    reachable: Mutex<Vec<NodeId>>,
    reachable_members: Mutex<Vec<NodeId>>,
}

impl ClusterFacts {
    pub(crate) fn update(&self, status: &NodeStatus) {
        self.leader.store(
            matches!(status.role, trembita_core::Role::Leader),
            Ordering::SeqCst,
        );
        *self.leader_id.lock().unwrap() = status.leader;
        self.voters.lock().unwrap().clone_from(&status.voters);
        self.learners.lock().unwrap().clone_from(&status.learners);
        self.reachable.lock().unwrap().clone_from(&status.reachable);
        self.reachable_members
            .lock()
            .unwrap()
            .clone_from(&status.reachable_members);
    }

    /// Current Raft leader hint (refreshed with consensus status).
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        *self.leader_id.lock().unwrap()
    }
}

impl ClusterState for ClusterFacts {
    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.voters.lock().unwrap().clone()
    }

    fn cluster_nodes(&self) -> Vec<NodeId> {
        let voters = self.voters.lock().unwrap();
        let learners = self.learners.lock().unwrap();
        let mut nodes = voters.clone();
        nodes.extend(learners.iter().copied());
        nodes.sort();
        nodes.dedup();
        nodes
    }

    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.reachable.lock().unwrap().clone()
    }

    fn placement_nodes(&self) -> Vec<NodeId> {
        self.reachable_members.lock().unwrap().clone()
    }

    fn leader_id(&self) -> Option<NodeId> {
        *self.leader_id.lock().unwrap()
    }
}
