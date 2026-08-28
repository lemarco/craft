//! Leader-only cluster supervisor (backlog E10,
//! [cluster-elasticity#supervisor--leader-only-reconciliation](../../../docs/decisions/cluster-elasticity.md#supervisor--leader-only-reconciliation)).
//!
//! Only the Raft **leader** runs cluster-wide actor placement. The
//! [`ClusterSupervisor`] holds a declarative set of *managed groups* — "keep
//! `total` instances of actor `A` alive, one per node" — and a
//! [`reconcile`](ClusterSupervisor::reconcile) step diffs that desired state
//! against the directory's current placement and the live membership, issuing
//! the spawns/stops through the E9 [`ClusterControl`]. Followers and candidates
//! skip reconciliation entirely (they return [`ReconcileReport::ran_as_leader`]
//! `= false`); non-leaders that receive a `scale_cluster` call forward it to the
//! leader in the runtime, mirroring client-request forwarding (client-routing).
//!
//! Reconciliation is **idempotent**: once the directory reflects the spawned
//! instances, a repeat reconcile plans no changes (supervisor-leader). It is triggered on
//! membership changes committing (membership-early), on reachability changes (liveness-vs-membership —
//! crash-driven respawn without waiting for a `ConfChange`), on
//! `scale_cluster` API calls, and — from E11 — on node join (auto-spawn, auto-spawn-on-join).
//!
//! The [`ClusterState`] port abstracts *where* leadership and membership come
//! from, so the supervisor is testable without a live consensus node; the
//! runtime supplies a real implementation when it embeds the supervisor.

use std::sync::{Arc, Mutex};

use craft_net::transport::BoxFuture;
use craft_proto::NodeId;

use crate::placement::{ClusterControl, ClusterScaleError, ScalePlan};
use crate::registry::UserActor;

/// The cluster facts a [`ClusterSupervisor`] reconciles against: whether this
/// node is the current Raft leader, and which nodes are live (the committed
/// voter set). Implemented by the node runtime; a mock suffices for tests.
pub trait ClusterState: Send + Sync {
    /// Whether this node is currently the Raft leader.
    fn is_leader(&self) -> bool;
    /// The current committed voter set (Raft membership). Used as the placement
    /// target: instances are only ever spawned onto committed voters.
    fn live_nodes(&self) -> Vec<NodeId>;
    /// The voters currently believed **reachable** — a liveness signal distinct
    /// from membership (liveness-vs-membership). Defaults to [`live_nodes`](Self::live_nodes)
    /// so an implementation with no failure detector behaves as before (every
    /// committed voter is assumed alive). The runtime overrides this with the
    /// leader's heartbeat-derived reachability, enabling crash detection without
    /// waiting for a `ConfChange`.
    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.live_nodes()
    }
    /// The current Raft leader hint for forwarding (queue wire, client routing).
    fn leader_id(&self) -> Option<NodeId> {
        None
    }
}

impl<T: ClusterState + ?Sized> ClusterState for Arc<T> {
    fn is_leader(&self) -> bool {
        (**self).is_leader()
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        (**self).live_nodes()
    }
    fn reachable_nodes(&self) -> Vec<NodeId> {
        (**self).reachable_nodes()
    }
    fn leader_id(&self) -> Option<NodeId> {
        (**self).leader_id()
    }
}

/// How many instances a managed group should keep cluster-wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// A fixed cluster-wide count (`manage`).
    Fixed(usize),
    /// One per **reachable** node — the count tracks liveness, not just
    /// membership (`manage_auto`, auto-spawn-on-join, liveness-vs-membership). A crashed-but-still-voter host
    /// drops out until it acks again; a newly reachable joiner gets a worker on
    /// the next reconcile.
    PerLiveNode,
}

impl Target {
    /// Resolve the desired total against the current reachable node count.
    fn resolve(self, reachable_count: usize) -> usize {
        match self {
            Target::Fixed(n) => n,
            Target::PerLiveNode => reachable_count,
        }
    }
}

/// A reconcile step for one managed group: given the control plane, the
/// resolved target count, and the live membership, drive the group to its
/// desired placement.
type ReconcileFn = Arc<
    dyn Fn(
            Arc<ClusterControl>,
            usize,
            Vec<NodeId>,
        ) -> BoxFuture<'static, Result<ScalePlan, ClusterScaleError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
struct ManagedSpec {
    name: String,
    target: Target,
    reconcile: ReconcileFn,
}

/// The outcome of a single managed group's reconcile.
#[derive(Debug)]
pub struct GroupReconcile {
    /// The group name.
    pub name: String,
    /// The desired cluster-wide instance count.
    pub total: usize,
    /// The plan executed (or the error that stopped it).
    pub result: Result<ScalePlan, ClusterScaleError>,
}

/// The result of a [`reconcile`](ClusterSupervisor::reconcile) pass.
#[derive(Debug)]
pub struct ReconcileReport {
    /// Whether this node acted as leader (`false` means the pass was skipped).
    pub ran_as_leader: bool,
    /// Per managed-group outcomes (empty when skipped).
    pub groups: Vec<GroupReconcile>,
}

impl ReconcileReport {
    /// Whether every managed group reconciled without error (also `true` for a
    /// skipped non-leader pass, which changed nothing).
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.groups.iter().all(|g| g.result.is_ok())
    }

    /// The total number of spawns issued across all groups this pass.
    #[must_use]
    pub fn spawns(&self) -> usize {
        self.groups
            .iter()
            .filter_map(|g| g.result.as_ref().ok())
            .map(|plan| plan.spawns.len())
            .sum()
    }
}

/// The leader-only orchestrator of cluster-wide actor placement (E10).
pub struct ClusterSupervisor<S: ClusterState> {
    control: Arc<ClusterControl>,
    state: S,
    managed: Mutex<Vec<ManagedSpec>>,
}

impl<S: ClusterState> ClusterSupervisor<S> {
    /// Create a supervisor over `control`, reading leadership/membership from
    /// `state`.
    #[must_use]
    pub fn new(control: Arc<ClusterControl>, state: S) -> Self {
        Self {
            control,
            state,
            managed: Mutex::new(Vec::new()),
        }
    }

    /// Declare that the cluster should keep exactly `total` instances of actor
    /// `A` named `name`, one per node (one-worker-per-vps). Registers `A`'s spawn factory
    /// on the local control plane so this node can host or place it. Every node
    /// runs the same managed set at startup, so any node that becomes leader
    /// can place the group.
    pub fn manage<A>(&self, name: &str, total: usize, config: A::Config)
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        self.push_managed::<A>(name, Target::Fixed(total), config);
    }

    /// Declare an **auto-worker** group (auto-spawn-on-join): one instance of `A` on every
    /// reachable node, with the count tracking liveness (liveness-vs-membership). A node that
    /// joins the cluster gets a worker on the next reconcile; a node that
    /// leaves or crashes has its instance planned for removal / respawned
    /// elsewhere. This is what makes `JOIN_ADDR` + the same binary bring a
    /// worker up automatically, with no `main` boilerplate.
    pub fn manage_auto<A>(&self, name: &str, config: A::Config)
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        self.push_managed::<A>(name, Target::PerLiveNode, config);
    }

    fn push_managed<A>(&self, name: &str, target: Target, config: A::Config)
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        self.control.register_type::<A>();
        let group = name.to_string();
        let reconcile: ReconcileFn = Arc::new(move |control: Arc<ClusterControl>, total, live| {
            let group = group.clone();
            let config = config.clone();
            Box::pin(async move {
                control
                    .scale_cluster::<A>(&group, total, config, &live)
                    .await
            })
        });
        self.managed.lock().unwrap().push(ManagedSpec {
            name: name.to_string(),
            target,
            reconcile,
        });
    }

    /// Names of the groups this supervisor manages.
    #[must_use]
    pub fn managed_names(&self) -> Vec<String> {
        self.managed
            .lock()
            .unwrap()
            .iter()
            .map(|m| m.name.clone())
            .collect()
    }

    /// Reconcile every managed group toward its desired placement — but only if
    /// this node is the leader (supervisor-leader). On a follower/candidate the pass is
    /// skipped and reports `ran_as_leader = false`.
    pub async fn reconcile(&self) -> ReconcileReport {
        if !self.state.is_leader() {
            return ReconcileReport {
                ran_as_leader: false,
                groups: Vec::new(),
            };
        }
        // Placement follows liveness, not just committed membership (liveness-vs-membership):
        // a crashed-but-still-voter host is excluded until it acks again.
        let reachable = self.state.reachable_nodes();
        let specs = self.managed.lock().unwrap().clone();
        let mut groups = Vec::with_capacity(specs.len());
        for spec in specs {
            let desired = spec.target.resolve(reachable.len());
            // One-worker-per-node (one-worker-per-vps) cannot exceed the reachable set.
            let total = desired.min(reachable.len());
            let result =
                (spec.reconcile)(Arc::clone(&self.control), total, reachable.clone()).await;
            groups.push(GroupReconcile {
                name: spec.name,
                total,
                result,
            });
        }
        ReconcileReport {
            ran_as_leader: true,
            groups,
        }
    }
}
