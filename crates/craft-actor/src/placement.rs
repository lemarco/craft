//! Actor control plane: remote spawn + cluster-wide placement (backlog E9,
//! [cross-node-actors](../../../docs/decisions/cross-node-actors.md),
//! [one-worker-per-vps](../../../docs/decisions/one-worker-per-vps.md)).
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
//!   (one-worker-per-vps): the pure [`plan_scale`] planner diffs the desired `total`
//!   against the directory's current placement and the live membership, and
//!   `scale_cluster` executes the resulting spawns.
//!
//! Planned *removals* (demoted or dead nodes) are returned in the [`ScalePlan`]
//! and applied locally where they target this node; tearing down instances on
//! *other* nodes is the leader-only `ClusterSupervisor`'s job (E10), which
//! reuses this same planner.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use craft_net::transport::{Body, BoxFuture};
use craft_net::{
    RemoteError, RequestHandler, Route, Transport, TransportError, decode_body, encode_body,
    send_actor_migrate, send_actor_scale, send_actor_spawn, send_actor_stop,
};
use craft_proto::{
    ActorId, ActorRegistration, ActorTypeId, MigrateReply, MigrateRequest, NodeId, ScaleReply,
    ScaleRequest, SpawnReply, SpawnRequest, StopReply, StopRequest,
};

use crate::directory::ActorDirectory;
use crate::registry::{
    ActorRegistry, ConfigCodecError, ScaleError, SnapshotError, SpawnError, StopError, UserActor,
};
use crate::supervisor::ClusterState;

// ---------------------------------------------------------------------------
// Placement planner (pure)
// ---------------------------------------------------------------------------

/// The changes required to bring a group to a target instance count (one-worker-per-vps).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScalePlan {
    /// Nodes that should each spawn one new instance of the group.
    pub spawns: Vec<NodeId>,
    /// Existing instances that should be stopped: extras on kept nodes, plus
    /// every instance on a demoted or dead node.
    pub removes: Vec<ActorId>,
}

/// Plan the placement to reach `total` instances of a group, **one worker per
/// node** (one-worker-per-vps production model), given the group's `current` registrations
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
    /// The request could not be shipped to the target node, or that node
    /// rejected the spawn.
    #[error(transparent)]
    Remote(#[from] RemoteError),
}

/// The [`ScaleReply::error`] a leader-gated [`handle_scale`](ClusterControl::handle_scale)
/// returns when this node is not (or is no longer) the leader. Distinct from a
/// planning/placement failure: it is **transient** — leadership is settling or
/// has moved — so a forwarding caller should re-resolve the leader and retry
/// rather than surface it (supervisor-leader).
pub const NOT_LEADER_REASON: &str = "not leader";

/// Why a [`scale_cluster`](ClusterControl::scale_cluster) failed (E9).
#[derive(Debug, thiserror::Error)]
pub enum ClusterScaleError {
    /// The placement could not be planned (e.g. too few nodes).
    #[error(transparent)]
    Plan(#[from] ScaleError),
    /// A spawn issued by the plan failed.
    #[error(transparent)]
    Spawn(#[from] RemoteSpawnError),
    /// A removal issued by the plan could not be enacted on a remote node
    /// (shipping failure or rejection).
    #[error(transparent)]
    Stop(#[from] RemoteError),
}

/// Why a [`migrate`](ClusterControl::migrate) failed (E12, cross-node-actors).
#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    /// The instance to migrate is not hosted on this node.
    #[error("instance {0:?} is not local to this node")]
    NotLocal(ActorId),
    /// The migration target is this node — nothing to migrate.
    #[error("cannot migrate to the same node {0:?}")]
    SameNode(NodeId),
    /// Capturing the source's migration snapshot failed.
    #[error("snapshot failed: {0}")]
    Snapshot(#[from] SnapshotError),
    /// The actor's config could not be encoded for shipping.
    #[error("config encode failed: {0}")]
    Config(ConfigCodecError),
    /// The migrate request could not be shipped to the target node, or that
    /// node rejected the replacement spawn.
    #[error(transparent)]
    Remote(#[from] RemoteError),
}

// ---------------------------------------------------------------------------
// Control plane
// ---------------------------------------------------------------------------

/// A factory that reconstructs an actor of a specific type from a wire config
/// and spawns it under `name`, returning the assigned instance id. A non-empty
/// `snapshot` restores migratable state before the actor runs (E12).
type SpawnFactory =
    Arc<dyn Fn(&ActorRegistry, &str, &[u8], &[u8]) -> Result<u32, SpawnError> + Send + Sync>;

fn make_factory<A: UserActor>() -> SpawnFactory {
    Arc::new(
        |registry: &ActorRegistry, name: &str, config: &[u8], snapshot: &[u8]| {
            let config = A::decode_config(config)?;
            if snapshot.is_empty() {
                registry.spawn::<A>(name, config)?;
            } else {
                registry.spawn_restoring::<A>(name, config, snapshot)?;
            }
            // A fresh singleton group is assigned instance id 0.
            Ok(0)
        },
    )
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
    // Optional leadership/membership view used to leader-gate forwarded scales
    // and to source authoritative `live_nodes` (supervisor-leader). `None` in tests /
    // sim that drive placement directly without a consensus node.
    state: Option<Arc<dyn ClusterState>>,
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
            state: None,
        }
    }

    /// Attach a [`ClusterState`] so forwarded scales are leader-gated and source
    /// their `live_nodes` from this node's own committed voters (supervisor-leader). The
    /// runtime wires the real one; without it, [`handle_scale`](Self::handle_scale)
    /// trusts the requester's view (test/sim behavior).
    #[must_use]
    pub fn with_cluster_state(mut self, state: Arc<dyn ClusterState>) -> Self {
        self.state = Some(state);
        self
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

    /// Spawn a single `A` named `name` on `node` (cross-node-actors). Local if `node` is
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
            // Idempotent (auto-spawn-on-join, supervisor-leader): a repeat local spawn of an existing
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
            .map_err(|e| RemoteSpawnError::Remote(RemoteError::transport(node, e)))?;
        match reply.id {
            Some(id) => Ok(id),
            None => Err(RemoteSpawnError::Remote(RemoteError::rejected(
                node,
                reply.error.unwrap_or_else(|| "unknown".to_string()),
            ))),
        }
    }

    /// Drive group `name` to `total` instances cluster-wide, one worker per node
    /// (one-worker-per-vps). Plans placement with [`plan_scale`] against the directory's
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
        self.enact_removes(name, &plan.removes).await?;
        Ok(plan)
    }

    /// The type-erased core of [`scale_cluster`](ClusterControl::scale_cluster):
    /// plan against `live_nodes` and execute the spawns from an **already
    /// encoded** config, reconstructing each instance through the `actor_type`
    /// factory (locally via [`handle_spawn`](ClusterControl::handle_spawn),
    /// remotely via a raw [`SpawnRequest`]). Used by the leader when it receives
    /// a forwarded [`ScaleRequest`], where the concrete actor type is not known
    /// statically. Every hosting node must have registered the type.
    ///
    /// # Errors
    /// Returns [`ClusterScaleError`] if placement cannot be planned or a spawn
    /// fails.
    pub async fn scale_cluster_encoded(
        &self,
        name: &str,
        actor_type: ActorTypeId,
        total: usize,
        config: &[u8],
        live_nodes: &[NodeId],
    ) -> Result<ScalePlan, ClusterScaleError> {
        let current = self.directory.lookup(name);
        let plan = plan_scale(total, live_nodes, &current)?;
        for &node in &plan.spawns {
            let request = SpawnRequest {
                name: name.to_string(),
                actor_type: actor_type.clone(),
                config: config.to_vec(),
                generation: 0,
            };
            if node == self.node_id {
                if let Some(reason) = self.handle_spawn(&request).error {
                    return Err(
                        RemoteSpawnError::Remote(RemoteError::rejected(node, reason)).into(),
                    );
                }
            } else {
                let reply = send_actor_spawn(self.transport.as_ref(), node, &request)
                    .await
                    .map_err(|e| RemoteSpawnError::Remote(RemoteError::transport(node, e)))?;
                if reply.id.is_none() {
                    return Err(RemoteSpawnError::Remote(RemoteError::rejected(
                        node,
                        reply.error.unwrap_or_else(|| "unknown".to_string()),
                    ))
                    .into());
                }
            }
        }
        self.enact_removes(name, &plan.removes).await?;
        Ok(plan)
    }

    /// Enact the [`ScalePlan`]'s removals: stop the group locally where a removal
    /// targets this node, and send an `/actor/stop` to every *other* node with a
    /// planned removal (cross-node-actors, supervisor-leader). One worker per node (one-worker-per-vps), so a removal
    /// on node N means "stop this group on N"; nodes are deduped so at most one
    /// stop is sent per node.
    ///
    /// # Errors
    /// Returns [`RemoteStopError`] if a remote stop cannot be shipped or the
    /// target rejects it. A departed node (unreachable) surfaces as a transport
    /// error; its instances are reaped when it leaves, and the next reconcile
    /// re-plans any residual removal.
    async fn enact_removes(&self, name: &str, removes: &[ActorId]) -> Result<(), RemoteError> {
        let mut remote_nodes: BTreeSet<NodeId> = BTreeSet::new();
        for id in removes {
            if id.node == self.node_id {
                let _ = self.registry.stop(name);
            } else {
                remote_nodes.insert(id.node);
            }
        }
        for node in remote_nodes {
            let request = StopRequest {
                name: name.to_string(),
            };
            let reply = send_actor_stop(self.transport.as_ref(), node, &request)
                .await
                .map_err(|e| RemoteError::transport(node, e))?;
            if let Some(reason) = reply.error {
                return Err(RemoteError::rejected(node, reason));
            }
        }
        Ok(())
    }

    /// Handle an inbound [`StopRequest`] and produce its [`StopReply`]: stop the
    /// named group locally. Idempotent — an absent group is reported as success
    /// (it is already gone), so a re-sent removal is harmless.
    #[must_use]
    pub fn handle_stop(&self, request: &StopRequest) -> StopReply {
        match self.registry.stop(&request.name) {
            Ok(()) | Err(StopError::NotFound(_)) => StopReply { error: None },
        }
    }

    /// Serve a forwarded [`ScaleRequest`] on the leader (`/actor/scale`, supervisor-leader).
    ///
    /// **Leader-gated:** when a [`ClusterState`] is attached
    /// ([`with_cluster_state`](Self::with_cluster_state)), this re-confirms the
    /// node is still the leader before planning — a node deposed mid-flight must
    /// not run cluster-wide placement concurrently with the real leader's
    /// reconcile (double-placement / spurious stops). It also sources
    /// `live_nodes` from **this node's own committed voters**, which are never
    /// staler than the requester's set (which may have lagged a `ConfChange`),
    /// avoiding under-placement or spawns onto a removed node. Without a state
    /// (tests/sim) it falls back to the requester's `live_nodes`.
    #[must_use]
    pub async fn handle_scale(&self, request: &ScaleRequest) -> ScaleReply {
        let live_nodes = match &self.state {
            Some(state) => {
                if !state.is_leader() {
                    return ScaleReply {
                        error: Some(NOT_LEADER_REASON.to_string()),
                    };
                }
                state.live_nodes()
            }
            None => request.live_nodes.clone(),
        };
        match self
            .scale_cluster_encoded(
                &request.name,
                request.actor_type.clone(),
                request.total as usize,
                &request.config,
                &live_nodes,
            )
            .await
        {
            Ok(_) => ScaleReply { error: None },
            Err(e) => ScaleReply {
                error: Some(e.to_string()),
            },
        }
    }

    /// Forward a [`ScaleRequest`] to the current `leader` and decode its
    /// [`ScaleReply`] (used when `scale_cluster` is called on a follower).
    ///
    /// # Errors
    /// Returns [`TransportError`] if the leader is unreachable or the send
    /// fails.
    pub async fn request_scale(
        &self,
        leader: NodeId,
        request: &ScaleRequest,
    ) -> Result<ScaleReply, TransportError> {
        send_actor_scale(self.transport.as_ref(), leader, request).await
    }

    /// Migrate the locally-hosted instance `from` to `to_node`, transferring
    /// its state (E12, cross-node-actors). Captures the instance's migration snapshot,
    /// asks the target to spawn a replacement (of type `A`) and restore it, then
    /// gracefully drains and stops the source with `drain_timeout` (drain-timeout).
    ///
    /// The replacement's generation is bumped past the source's so stale
    /// references are detectable.
    ///
    /// # Errors
    /// Returns [`MigrateError`] if the instance is not local, the target is this
    /// node, the snapshot / config-encode fails, the transport fails, or the
    /// target rejects the spawn.
    pub async fn migrate<A: UserActor>(
        &self,
        from: ActorId,
        to_node: NodeId,
        config: A::Config,
        drain_timeout: Duration,
    ) -> Result<ActorId, MigrateError> {
        if from.node != self.node_id {
            return Err(MigrateError::NotLocal(from));
        }
        if to_node == self.node_id {
            return Err(MigrateError::SameNode(to_node));
        }
        let snapshot = self
            .registry
            .snapshot_local(&from.name, from.instance)
            .await?;
        let config = A::encode_config(&config).map_err(MigrateError::Config)?;
        let request = MigrateRequest {
            from: from.clone(),
            name: from.name.clone(),
            actor_type: Self::type_id::<A>(),
            config,
            snapshot,
            generation: from.generation + 1,
        };
        let reply = send_actor_migrate(self.transport.as_ref(), to_node, &request)
            .await
            .map_err(|e| MigrateError::Remote(RemoteError::transport(to_node, e)))?;
        let Some(id) = reply.id else {
            return Err(MigrateError::Remote(RemoteError::rejected(
                to_node,
                reply.error.unwrap_or_else(|| "unknown".to_string()),
            )));
        };
        // Target has the state as of the snapshot; drain and stop the source.
        let _ = self.registry.stop_graceful(&from.name, drain_timeout).await;
        Ok(id)
    }

    /// Handle an inbound [`MigrateRequest`] and produce its [`MigrateReply`]:
    /// spawn a replacement of the requested type and restore the snapshot into
    /// it before it handles any message (E12). Idempotent — a replacement that
    /// already exists is reported as success.
    #[must_use]
    pub fn handle_migrate(&self, request: &MigrateRequest) -> MigrateReply {
        let factory = self
            .factories
            .lock()
            .unwrap()
            .get(&request.actor_type)
            .cloned();
        let Some(factory) = factory else {
            return MigrateReply {
                id: None,
                error: Some(SpawnError::UnknownType(request.actor_type.0.clone()).to_string()),
            };
        };
        match factory(
            &self.registry,
            &request.name,
            &request.config,
            &request.snapshot,
        ) {
            Ok(instance) => MigrateReply {
                id: Some(ActorId {
                    node: self.node_id,
                    name: request.name.clone(),
                    instance,
                    generation: request.generation,
                }),
                error: None,
            },
            Err(SpawnError::NameExists(_)) => MigrateReply {
                id: Some(ActorId {
                    node: self.node_id,
                    name: request.name.clone(),
                    instance: 0,
                    generation: request.generation,
                }),
                error: None,
            },
            Err(e) => MigrateReply {
                id: None,
                error: Some(e.to_string()),
            },
        }
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
        let outcome = factory(&self.registry, &request.name, &request.config, &[]);
        match outcome {
            // A fresh spawn, or an idempotent repeat of one already present
            // (cross-node-actors idempotent spawn by name/node/generation), both succeed.
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
            Route::ActorMigrate => decode_body::<MigrateRequest>(&body)
                .map_err(TransportError::from)
                .and_then(|request| Ok(encode_body(&self.handle_migrate(&request))?)),
            Route::ActorStop => decode_body::<StopRequest>(&body)
                .map_err(TransportError::from)
                .and_then(|request| Ok(encode_body(&self.handle_stop(&request))?)),
            other => Err(TransportError::Io(format!(
                "control handler received unexpected route {other:?}"
            ))),
        };
        Box::pin(async move { result })
    }
}
