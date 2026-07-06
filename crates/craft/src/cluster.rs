//! [`CraftCluster`] — the running node handle returned by the builder.
//!
//! It bundles everything the facade wired together: the consensus/actor runtime
//! (via an in-process [`NodeHandle`] for zero-copy L1 clients), the actor
//! control/messaging/directory planes, the leader-only supervisor, and the
//! telemetry [`EventBus`] + [`Metrics`]. Background tasks (facts refresh,
//! directory anti-entropy, supervisor reconcile, admin server) run until
//! [`shutdown`](CraftCluster::shutdown) or the handle is dropped.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use craft_core::StateMachine;
use craft_dashboard::{EventBus, Metrics};
use craft_proto::NodeId;
use tokio::task::JoinHandle;

use craft_actor::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterMessaging, ClusterState,
    ClusterSupervisor, DirectorySync, NodeHandle, NodeStatus,
};

/// The live leadership/membership facts the supervisor reconciles against
/// (implements [`ClusterState`]), refreshed from the node's consensus status by
/// a background task. Exposed only so [`CraftCluster::supervisor`] has a nameable
/// type; you rarely construct or read it directly.
#[derive(Default)]
pub struct ClusterFacts {
    leader: AtomicBool,
    voters: Mutex<Vec<NodeId>>,
}

impl ClusterFacts {
    pub(crate) fn update(&self, status: &NodeStatus) {
        self.leader.store(
            matches!(status.role, craft_core::Role::Leader),
            Ordering::SeqCst,
        );
        *self.voters.lock().unwrap() = status.voters.clone();
    }
}

impl ClusterState for ClusterFacts {
    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.voters.lock().unwrap().clone()
    }
}

/// A running craft node: the facade's single entry point once
/// [`start`](crate::CraftClusterBuilder::start_local) returns.
///
/// Clone-free but cheap to pass by reference; drop it (or call
/// [`shutdown`](Self::shutdown)) to stop the node.
pub struct CraftCluster<M: StateMachine> {
    pub(crate) node_id: NodeId,
    pub(crate) handle: NodeHandle<M>,
    pub(crate) registry: ActorRegistry,
    pub(crate) control: Arc<ClusterControl>,
    pub(crate) messaging: Arc<ClusterMessaging>,
    pub(crate) directory: Arc<ActorDirectory>,
    pub(crate) directory_sync: Arc<DirectorySync>,
    pub(crate) supervisor: Arc<ClusterSupervisor<Arc<ClusterFacts>>>,
    pub(crate) events: EventBus,
    pub(crate) metrics: Metrics,
    pub(crate) members: Vec<NodeId>,
    pub(crate) tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<M: StateMachine> CraftCluster<M> {
    /// Start describing a node running `machine`, identified by `node_id`. See
    /// [`CraftClusterBuilder`](crate::CraftClusterBuilder) for the options.
    #[must_use]
    pub fn builder(node_id: NodeId, machine: M) -> crate::CraftClusterBuilder<M> {
        crate::CraftClusterBuilder::new(node_id, machine)
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The configured cluster membership (initial voters).
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// The in-process client handle (the L1 client of ADR 002): propose and
    /// query the replicated state machine with no network hop when this node is
    /// the leader.
    #[must_use]
    pub fn handle(&self) -> &NodeHandle<M> {
        &self.handle
    }

    /// The node-local actor registry (spawn / scale / drain local actors).
    #[must_use]
    pub fn registry(&self) -> &ActorRegistry {
        &self.registry
    }

    /// The cluster control plane (remote spawn, cluster-wide scale, migration).
    #[must_use]
    pub fn control(&self) -> &Arc<ClusterControl> {
        &self.control
    }

    /// Cross-node actor messaging (round-robin and keyed casts).
    #[must_use]
    pub fn messaging(&self) -> &Arc<ClusterMessaging> {
        &self.messaging
    }

    /// The cluster-wide actor directory.
    #[must_use]
    pub fn directory(&self) -> &Arc<ActorDirectory> {
        &self.directory
    }

    /// The leader-only supervisor driving managed / auto-worker groups.
    #[must_use]
    pub fn supervisor(&self) -> &Arc<ClusterSupervisor<Arc<ClusterFacts>>> {
        &self.supervisor
    }

    /// The telemetry event bus; subscribe with [`EventBus::subscribe`].
    #[must_use]
    pub fn events(&self) -> &EventBus {
        &self.events
    }

    /// The Prometheus metrics registry served on the admin port.
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// A point-in-time consensus status snapshot, or `None` if the runtime has
    /// stopped.
    pub async fn status(&self) -> Option<NodeStatus> {
        self.handle.status().await
    }

    /// Whether this node currently believes it is the Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.handle
            .status()
            .await
            .is_some_and(|s| matches!(s.role, craft_core::Role::Leader))
    }

    /// Publish this node's local actor registrations to the rest of the cluster
    /// once, immediately (the runtime also does this periodically). Returns the
    /// number of peers that acknowledged.
    pub async fn publish_directory(&self) -> usize {
        let regs = self.registry.local_registrations(self.node_id);
        self.directory_sync.publish(&self.members, regs).await
    }

    /// Stop the node: shut the runtime down and abort all background tasks.
    pub fn shutdown(&self) {
        self.handle.shutdown();
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

impl<M: StateMachine> Drop for CraftCluster<M> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
