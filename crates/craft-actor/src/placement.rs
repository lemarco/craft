//! Actor control plane: remote spawn + cluster-wide placement (backlog E9,
//! [ADR 013](../../../docs/decisions/013-cross-node-actors.md),
//! [ADR 014](../../../docs/decisions/014-one-worker-per-vps.md)).
//!
//! [`ClusterControl`] places actors across the cluster:
//!
//! * [`spawn_remote`](ClusterControl::spawn_remote) starts one actor on a
//!   named node — locally if that is us, otherwise via a [`SpawnRequest`] over
//!   `/actor/spawn`. The target reconstructs the actor from a **factory**
//!   registered for its type ([`register_type`](ClusterControl::register_type)),
//!   since a node cannot spawn a type it was never told about.
//! * [`scale_cluster`](ClusterControl::scale_cluster) drives a group to a
//!   cluster-wide instance count using the **one-worker-per-node** model
//!   (ADR 014): the pure [`plan_scale`] planner diffs the desired `total`
//!   against the directory's current placement and the live membership, and
//!   `scale_cluster` executes the resulting spawns.
//!
//! Planned *removals* (demoted or dead nodes) are returned in the [`ScalePlan`]
//! and applied locally where they target this node; tearing down instances on
//! *other* nodes is the leader-only `ClusterSupervisor`'s job (E10), which
//! reuses this same planner.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use craft_net::transport::{Body, BoxFuture};
use craft_net::{
    RequestHandler, Route, Transport, TransportError, decode_body, encode_body, send_actor_spawn,
};
use craft_proto::{ActorId, ActorRegistration, ActorTypeId, NodeId, SpawnReply, SpawnRequest};

use crate::directory::ActorDirectory;
use crate::registry::{ActorRegistry, ConfigCodecError, ScaleError, SpawnError, UserActor};

// ---------------------------------------------------------------------------
// Placement planner (pure)
// ---------------------------------------------------------------------------

/// The changes required to bring a group to a target instance count (ADR 014).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScalePlan {
    /// Nodes that should each spawn one new instance of the group.
    pub spawns: Vec<NodeId>,
    /// Existing instances that should be stopped: extras on kept nodes, plus
    /// every instance on a demoted or dead node.
    pub removes: Vec<ActorId>,
}

/// Plan the placement to reach `total` instances of a group, **one worker per
/// node** (ADR 014 production model), given the group's `current` registrations
/// (cluster-wide, from the directory) and the current `live_nodes` (Raft
/// membership).
///
/// Placement is stable and deterministic: nodes that already host the group are
/// kept (up to `total`, lowest [`NodeId`] first), new instances fill the
/// remaining live nodes in [`NodeId`] order, and everything else is scheduled
/// for removal (extra instances on a kept node, instances on non-kept or dead
/// nodes).
///
/// # Errors
/// Returns [`ScaleError::InsufficientNodes`] if `total` exceeds the number of
/// live nodes (one-per-node cannot be satisfied).
pub fn plan_scale(
    total: usize,
    live_nodes: &[NodeId],
    current: &[ActorRegistration],
) -> Result<ScalePlan, ScaleError> {
    let mut live: Vec<NodeId> = live_nodes.to_vec();
    live.sort();
    live.dedup();

    if total > live.len() {
        return Err(ScaleError::InsufficientNodes {
            total,
            nodes: live.len(),
        });
    }

    // Group current instances by node, instances ascending.
    let mut by_node: BTreeMap<NodeId, Vec<ActorId>> = BTreeMap::new();
    for reg in current {
        by_node.entry(reg.id.node).or_default().push(reg.id.clone());
    }
    for ids in by_node.values_mut() {
        ids.sort();
    }

    // Keep live nodes that already host, lowest NodeId first, up to `total`.
    let keep: Vec<NodeId> = live
        .iter()
        .copied()
        .filter(|n| by_node.contains_key(n))
        .take(total)
        .collect();
    let keep_set: HashSet<NodeId> = keep.iter().copied().collect();

    // Removals: on kept nodes drop all but the first instance; on every other
    // node (demoted or dead) drop all instances.
    let mut removes = Vec::new();
    for (node, ids) in &by_node {
        let skip = usize::from(keep_set.contains(node));
        removes.extend(ids.iter().skip(skip).cloned());
    }

    // Spawns: fill the shortfall on live nodes that are not kept.
    let need = total - keep.len();
    let spawns: Vec<NodeId> = live
        .iter()
        .copied()
        .filter(|n| !keep_set.contains(n))
        .take(need)
        .collect();

    Ok(ScalePlan { spawns, removes })
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a [`spawn_remote`](ClusterControl::spawn_remote) failed (E9).
#[derive(Debug, thiserror::Error)]
pub enum RemoteSpawnError {
    /// A local spawn (target node is us) failed.
    #[error(transparent)]
    Local(#[from] SpawnError),
    /// The actor's config could not be encoded for shipping.
    #[error("config encode failed: {0}")]
    Config(ConfigCodecError),
    /// The envelope could not be shipped to the target node.
    #[error("transport to {node:?} failed: {reason}")]
    Transport {
        /// The target node.
        node: NodeId,
        /// The underlying transport error.
        reason: String,
    },
    /// The target node received the request but could not spawn.
    #[error("node {node:?} rejected spawn: {reason}")]
    Remote {
        /// The target node.
        node: NodeId,
        /// The reason it reported.
        reason: String,
    },
}

/// Why a [`scale_cluster`](ClusterControl::scale_cluster) failed (E9).
#[derive(Debug, thiserror::Error)]
pub enum ClusterScaleError {
    /// The placement could not be planned (e.g. too few nodes).
    #[error(transparent)]
    Plan(#[from] ScaleError),
    /// A spawn issued by the plan failed.
    #[error(transparent)]
    Spawn(#[from] RemoteSpawnError),
}

// ---------------------------------------------------------------------------
// Control plane
// ---------------------------------------------------------------------------

/// A factory that reconstructs an actor of a specific type from a wire config
/// and spawns it under `name`, returning the assigned instance id.
type SpawnFactory =
    Arc<dyn Fn(&ActorRegistry, &str, &[u8]) -> Result<u32, SpawnError> + Send + Sync>;

fn make_factory<A: UserActor>() -> SpawnFactory {
    Arc::new(|registry: &ActorRegistry, name: &str, config: &[u8]| {
        let config = A::decode_config(config)?;
        registry.spawn::<A>(name, config)?;
        // A fresh singleton group is assigned instance id 0.
        Ok(0)
    })
}

/// The node-local actor control plane (E9): remote spawn and cluster-wide
/// placement. Register every actor type the node may host with
/// [`register_type`](ClusterControl::register_type) so remote spawns can be
/// reconstructed. Serves `/actor/spawn` as a [`RequestHandler`].
pub struct ClusterControl {
    node_id: NodeId,
    registry: ActorRegistry,
    directory: Arc<ActorDirectory>,
    transport: Arc<dyn Transport>,
    factories: Mutex<HashMap<ActorTypeId, SpawnFactory>>,
}

impl ClusterControl {
    /// Create a control plane for `node_id` over `transport`, spawning into
    /// `registry` and reading current placement from `directory`.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        registry: ActorRegistry,
        directory: Arc<ActorDirectory>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            registry,
            directory,
            transport,
            factories: Mutex::new(HashMap::new()),
        }
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The type tag used to address actor `A` across the cluster.
    #[must_use]
    pub fn type_id<A: UserActor>() -> ActorTypeId {
        ActorTypeId(std::any::type_name::<A>().to_string())
    }

    /// Register a factory so this node can spawn `A` on request (locally or via
    /// `/actor/spawn`). Idempotent — a repeat registration replaces the prior.
    pub fn register_type<A: UserActor>(&self) {
        self.factories
            .lock()
            .unwrap()
            .insert(Self::type_id::<A>(), make_factory::<A>());
    }

    /// Spawn a single `A` named `name` on `node` (ADR 013). Local if `node` is
    /// this node, otherwise a [`SpawnRequest`] over `/actor/spawn`.
    ///
    /// # Errors
    /// Returns [`RemoteSpawnError`] on a local start failure, a config-encode
    /// failure, a transport failure, or rejection by the target node.
    pub async fn spawn_remote<A: UserActor>(
        &self,
        node: NodeId,
        name: &str,
        config: A::Config,
    ) -> Result<ActorId, RemoteSpawnError> {
        if node == self.node_id {
            // Idempotent (ADR 015/018): a repeat local spawn of an existing
            // group is a no-op, so reconciliation can run safely even before
            // the directory reflects the placement.
            match self.registry.spawn::<A>(name, config) {
                Ok(_) | Err(SpawnError::NameExists(_)) => {}
                Err(e) => return Err(e.into()),
            }
            return Ok(ActorId {
                node,
                name: name.to_string(),
                instance: 0,
                generation: 0,
            });
        }
        let config = A::encode_config(&config).map_err(RemoteSpawnError::Config)?;
        let request = SpawnRequest {
            name: name.to_string(),
            actor_type: Self::type_id::<A>(),
            config,
            generation: 0,
        };
        let reply = send_actor_spawn(self.transport.as_ref(), node, &request)
            .await
            .map_err(|e| RemoteSpawnError::Transport {
                node,
                reason: e.to_string(),
            })?;
        match reply.id {
            Some(id) => Ok(id),
            None => Err(RemoteSpawnError::Remote {
                node,
                reason: reply.error.unwrap_or_else(|| "unknown".to_string()),
            }),
        }
    }

    /// Drive group `name` to `total` instances cluster-wide, one worker per node
    /// (ADR 014). Plans placement with [`plan_scale`] against the directory's
    /// current view and `live_nodes`, executes the spawns (of type `A`), and
    /// applies removals that target this node. Returns the full plan so the
    /// caller / supervisor can enact remote removals (E10).
    ///
    /// # Errors
    /// Returns [`ClusterScaleError`] if placement cannot be planned or a spawn
    /// fails.
    pub async fn scale_cluster<A: UserActor>(
        &self,
        name: &str,
        total: usize,
        config: A::Config,
        live_nodes: &[NodeId],
    ) -> Result<ScalePlan, ClusterScaleError>
    where
        A::Config: Clone,
    {
        let current = self.directory.lookup(name);
        let plan = plan_scale(total, live_nodes, &current)?;
        for &node in &plan.spawns {
            self.spawn_remote::<A>(node, name, config.clone()).await?;
        }
        for id in &plan.removes {
            if id.node == self.node_id {
                let _ = self.registry.stop(name);
            }
        }
        Ok(plan)
    }

    /// Handle an inbound [`SpawnRequest`] and produce its [`SpawnReply`].
    #[must_use]
    pub fn handle_spawn(&self, request: &SpawnRequest) -> SpawnReply {
        let factory = self
            .factories
            .lock()
            .unwrap()
            .get(&request.actor_type)
            .cloned();
        let Some(factory) = factory else {
            return SpawnReply {
                id: None,
                error: Some(SpawnError::UnknownType(request.actor_type.0.clone()).to_string()),
            };
        };
        let outcome = factory(&self.registry, &request.name, &request.config);
        match outcome {
            // A fresh spawn, or an idempotent repeat of one already present
            // (ADR 013 idempotent spawn by name/node/generation), both succeed.
            Ok(instance) => SpawnReply {
                id: Some(ActorId {
                    node: self.node_id,
                    name: request.name.clone(),
                    instance,
                    generation: request.generation,
                }),
                error: None,
            },
            Err(SpawnError::NameExists(_)) => SpawnReply {
                id: Some(ActorId {
                    node: self.node_id,
                    name: request.name.clone(),
                    instance: 0,
                    generation: request.generation,
                }),
                error: None,
            },
            Err(e) => SpawnReply {
                id: None,
                error: Some(e.to_string()),
            },
        }
    }
}

impl RequestHandler for ClusterControl {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        let result = match route {
            Route::ActorSpawn => decode_body::<SpawnRequest>(&body)
                .map_err(TransportError::from)
                .and_then(|request| Ok(encode_body(&self.handle_spawn(&request))?)),
            other => Err(TransportError::Io(format!(
                "control handler received unexpected route {other:?}"
            ))),
        };
        Box::pin(async move { result })
    }
}
