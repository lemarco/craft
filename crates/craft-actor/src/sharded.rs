//! Multi-Raft node routing — shard-aware client dispatch and group-scoped peer
//! RPC demux (write-sharding-multi-raft).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use craft_core::{
    RaftGroupId, ShardCountExpansionPlan, ShardExpansionError, ShardId, ShardRouter,
    ShardRoutingKind, ShardRoutingSwitchError, ShardRoutingSwitchPlan, StableShardActivationError,
    StableShardActivationPlan, StableShardRouter, StateMachine, place_shard,
    plan_switch_to_stable_routing,
};
use craft_net::transport::{Body, BoxFuture, RequestHandler};
use craft_net::{Route, Transport, TransportError, decode_body, encode_body};
use craft_proto::{ClientRequest, ClientResponse, GroupPeerEnvelope, NodeId};
use craft_storage::{GroupRedbLayout, RaftStorage, StorageError};

use crate::RuntimeConfig;

/// Internal keyed-router state (Tier 1 modulus vs Tier 2 stable virtual prefix).
#[derive(Debug, Clone, Copy)]
enum KeyedRouter {
    Modulus(ShardRouter),
    Stable(StableShardRouter),
}

impl KeyedRouter {
    fn new(kind: ShardRoutingKind, active_count: u32) -> Self {
        match kind {
            ShardRoutingKind::Modulus => Self::Modulus(ShardRouter::new(active_count)),
            ShardRoutingKind::StableVirtual => Self::Stable(StableShardRouter::new(active_count)),
        }
    }

    fn kind(&self) -> ShardRoutingKind {
        match self {
            Self::Modulus(_) => ShardRoutingKind::Modulus,
            Self::Stable(_) => ShardRoutingKind::StableVirtual,
        }
    }

    fn active_count(&self) -> u32 {
        match self {
            Self::Modulus(r) => r.shard_count(),
            Self::Stable(r) => r.active_count(),
        }
    }

    fn shard_for(&self, key: &[u8]) -> Option<ShardId> {
        match self {
            Self::Modulus(r) => Some(r.shard_for(key)),
            Self::Stable(r) => r.shard_for(key),
        }
    }

    fn expand_shard_count(
        &mut self,
        new_count: u32,
    ) -> Result<ShardCountExpansionPlan, ShardExpansionError> {
        match self {
            Self::Modulus(r) => r.expand_shard_count(new_count),
            Self::Stable(_) => Err(ShardExpansionError::StableRoutingActive),
        }
    }

    fn activate_shards(
        &mut self,
        new_active: u32,
    ) -> Result<StableShardActivationPlan, StableShardActivationError> {
        match self {
            Self::Stable(r) => r.activate_shards(new_active),
            Self::Modulus(_) => Err(StableShardActivationError::ModulusRoutingActive),
        }
    }

    fn switch_to_stable(&mut self) -> Result<ShardRoutingSwitchPlan, ShardRoutingSwitchError> {
        let plan = plan_switch_to_stable_routing(self.kind(), self.active_count())?;
        *self = Self::Stable(StableShardRouter::new(plan.active_count));
        Ok(plan)
    }
}

/// Routes client and peer traffic to one of several Raft groups hosted on the
/// same physical node.
pub struct ShardedNodeService {
    router: RwLock<KeyedRouter>,
    group_ids: Arc<RwLock<Vec<RaftGroupId>>>,
    groups: Arc<RwLock<BTreeMap<u32, Arc<dyn RequestHandler>>>>,
}

impl ShardedNodeService {
    /// Build a sharded handler from per-group services.
    #[must_use]
    pub fn new(
        shard_count: u32,
        routing: ShardRoutingKind,
        group_ids: Vec<RaftGroupId>,
        groups: BTreeMap<u32, Arc<dyn RequestHandler>>,
    ) -> Self {
        Self {
            router: RwLock::new(KeyedRouter::new(routing, shard_count)),
            group_ids: Arc::new(RwLock::new(group_ids)),
            groups: Arc::new(RwLock::new(groups)),
        }
    }

    /// Keyed routing mode on this node.
    #[must_use]
    pub fn routing_kind(&self) -> ShardRoutingKind {
        self.router.read().expect("sharded router lock").kind()
    }

    /// Active shard count for keyed routing.
    #[must_use]
    pub fn shard_count(&self) -> u32 {
        self.router
            .read()
            .expect("sharded router lock")
            .active_count()
    }

    /// Expand the active shard keyspace (Tier 1 modulus). Keys remap — drain clients first.
    ///
    /// # Errors
    /// Returns [`ShardExpansionError`] when `new_count` is invalid or stable routing is active.
    pub fn expand_shard_count(
        &self,
        new_count: u32,
    ) -> Result<ShardCountExpansionPlan, ShardExpansionError> {
        self.router
            .write()
            .expect("sharded router lock")
            .expand_shard_count(new_count)
    }

    /// Grow the active virtual shard prefix (Tier 2 stable). Existing keys keep their shard id.
    ///
    /// # Errors
    /// Returns [`StableShardActivationError`] when `new_active` is invalid or modulus routing is active.
    pub fn activate_shards(
        &self,
        new_active: u32,
    ) -> Result<StableShardActivationPlan, StableShardActivationError> {
        self.router
            .write()
            .expect("sharded router lock")
            .activate_shards(new_active)
    }

    /// Switch keyed routing from Tier 1 modulus to Tier 2 stable virtual.
    ///
    /// Keys **remap** — drain clients before calling.
    ///
    /// # Errors
    /// Returns [`ShardRoutingSwitchError`] when stable routing is already active.
    pub fn switch_to_stable_routing(
        &self,
    ) -> Result<ShardRoutingSwitchPlan, ShardRoutingSwitchError> {
        self.router
            .write()
            .expect("sharded router lock")
            .switch_to_stable()
    }

    /// Raft groups currently hosted on this physical node.
    #[must_use]
    pub fn hosted_group_ids(&self) -> Vec<RaftGroupId> {
        self.groups
            .read()
            .expect("sharded groups lock")
            .keys()
            .copied()
            .map(RaftGroupId)
            .collect()
    }

    /// Register a newly spawned group handler.
    pub fn insert_group(&self, group: u32, handler: Arc<dyn RequestHandler>) {
        self.groups
            .write()
            .expect("sharded groups lock")
            .insert(group, handler);
        let mut ids = self.group_ids.write().expect("sharded group_ids lock");
        if !ids.iter().any(|g| g.0 == group) {
            ids.push(RaftGroupId(group));
            ids.sort_by_key(|g| g.0);
        }
    }

    /// Remove a group handler during rebalance.
    pub fn remove_group(&self, group: u32) -> Option<Arc<dyn RequestHandler>> {
        self.groups
            .write()
            .expect("sharded groups lock")
            .remove(&group)
    }

    /// Extend the routing catalog without requiring hosted handlers (Tier 2).
    pub fn extend_routing_catalog(&self, new_groups: &[RaftGroupId]) {
        let mut ids = self.group_ids.write().expect("sharded group_ids lock");
        for group in new_groups {
            if !ids.iter().any(|g| g.0 == group.0) {
                ids.push(*group);
            }
        }
        ids.sort_by_key(|g| g.0);
    }

    fn group_for_key(&self, key: &[u8]) -> Option<RaftGroupId> {
        let shard = self
            .router
            .read()
            .expect("sharded router lock")
            .shard_for(key)?;
        let ids = self.group_ids.read().expect("sharded group_ids lock");
        place_shard(shard, &ids)
    }

    fn route_client(&self, request: ClientRequest) -> Result<(u32, ClientRequest), ClientResponse> {
        let ids = self.group_ids.read().expect("sharded group_ids lock");
        match request {
            ClientRequest::Propose(bytes) => Ok((
                ids.first().map(|g| g.0).unwrap_or(0),
                ClientRequest::Propose(bytes),
            )),
            ClientRequest::Query(bytes) => Ok((
                ids.first().map(|g| g.0).unwrap_or(0),
                ClientRequest::Query(bytes),
            )),
            ClientRequest::ProposeKeyed { key, command } => {
                let Some(group) = self.group_for_key(&key) else {
                    return Err(ClientResponse::Error(
                        "key outside active shard range".into(),
                    ));
                };
                Ok((group.0, ClientRequest::Propose(command)))
            }
            ClientRequest::QueryKeyed { key, query } => {
                let Some(group) = self.group_for_key(&key) else {
                    return Err(ClientResponse::Error(
                        "key outside active shard range".into(),
                    ));
                };
                Ok((group.0, ClientRequest::Query(query)))
            }
            ClientRequest::ReadIndexConfirm { route_key: None } => Ok((
                ids.first().map(|g| g.0).unwrap_or(0),
                ClientRequest::ReadIndexConfirm { route_key: None },
            )),
            ClientRequest::ReadIndexConfirm {
                route_key: Some(key),
            } => {
                let Some(group) = self.group_for_key(&key) else {
                    return Err(ClientResponse::Error(
                        "key outside active shard range".into(),
                    ));
                };
                Ok((
                    group.0,
                    ClientRequest::ReadIndexConfirm {
                        route_key: Some(key),
                    },
                ))
            }
        }
    }
}

impl RequestHandler for ShardedNodeService {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        match route {
            Route::PeerWire => {
                let groups = Arc::clone(&self.groups);
                Box::pin(async move {
                    let envelope: GroupPeerEnvelope = decode_body(&body)?;
                    let handler = {
                        let groups = groups.read().expect("sharded groups lock");
                        groups.get(&envelope.group).cloned()
                    };
                    let Some(handler) = handler else {
                        return Err(TransportError::Io(format!(
                            "unknown raft group {}",
                            envelope.group
                        )));
                    };
                    handler
                        .handle(
                            Route::PeerWire,
                            encode_body(&envelope.rpc).map_err(TransportError::Wire)?,
                        )
                        .await
                })
            }
            Route::ClientWire => {
                let groups = Arc::clone(&self.groups);
                let request: ClientRequest = match decode_body(&body) {
                    Ok(r) => r,
                    Err(e) => {
                        return Box::pin(async move { Err(TransportError::Wire(e)) });
                    }
                };
                let routed = self.route_client(request);
                Box::pin(async move {
                    let (group_id, inner) = match routed {
                        Ok(pair) => pair,
                        Err(response) => return Ok(encode_body(&response)?),
                    };
                    let handler = {
                        let groups = groups.read().expect("sharded groups lock");
                        groups.get(&group_id).cloned()
                    };
                    let Some(handler) = handler else {
                        return Err(TransportError::Io(format!("unknown raft group {group_id}")));
                    };
                    handler
                        .handle(
                            Route::ClientWire,
                            encode_body(&inner).map_err(TransportError::Wire)?,
                        )
                        .await
                })
            }
            Route::ClusterJoin | Route::ClusterLeave | Route::ClusterCatalogAdd => {
                let groups = Arc::clone(&self.groups);
                Box::pin(async move {
                    let handler = {
                        let groups = groups.read().expect("sharded groups lock");
                        groups
                            .get(&0)
                            .cloned()
                            .or_else(|| groups.values().next().cloned())
                    };
                    let Some(handler) = handler else {
                        return Err(TransportError::Io("no raft groups".into()));
                    };
                    handler.handle(route, body).await
                })
            }
            other => {
                let groups = Arc::clone(&self.groups);
                Box::pin(async move {
                    let handler = {
                        let groups = groups.read().expect("sharded groups lock");
                        groups.values().next().cloned()
                    };
                    let Some(handler) = handler else {
                        return Err(TransportError::Io("no raft groups".into()));
                    };
                    handler.handle(other, body).await
                })
            }
        }
    }
}

/// Spawn one Raft group on a physical node, restoring from a migration bundle.
#[allow(clippy::too_many_arguments)]
pub fn spawn_raft_group_from_bundle<M>(
    node_id: NodeId,
    members: &[NodeId],
    group: u32,
    raft: craft_core::Config,
    runtime: RuntimeConfig,
    network: Arc<dyn Transport>,
    forward_timeout: Duration,
    storage_dir: Option<&Path>,
    bundle: &craft_proto::GroupMigrationBundle,
) -> Result<(Arc<dyn RequestHandler>, crate::NodeHandle<M>), StorageError>
where
    M: StateMachine + Default + 'static,
{
    use craft_net::GroupTransport;

    use crate::{NodeService, RaftDriver, spawn_node};

    let storage: Box<dyn RaftStorage> = if let Some(dir) = storage_dir {
        let mut storage = GroupRedbLayout::new(dir).open_group(group)?;
        craft_storage::import_migration(&mut storage, bundle)?;
        Box::new(storage)
    } else {
        craft_storage::import_migration_boxed(bundle)?
    };
    let machine = M::default();
    let driver = RaftDriver::recover(node_id, members.iter().copied(), raft, machine, storage)
        .map_err(|e| StorageError::Backend(e.to_string()))?;
    let group_transport = Arc::new(GroupTransport::new(group, network)) as Arc<dyn Transport>;
    let handle = spawn_node(driver, Arc::clone(&group_transport), runtime);
    let service = Arc::new(
        NodeService::new(handle.clone(), group_transport).with_forward_timeout(forward_timeout),
    ) as Arc<dyn RequestHandler>;
    Ok((service, handle))
}

/// Spawn one Raft group on a physical node.
#[allow(clippy::too_many_arguments)]
pub fn spawn_raft_group<M>(
    node_id: NodeId,
    members: &[NodeId],
    group: u32,
    raft: craft_core::Config,
    runtime: RuntimeConfig,
    machine: M,
    network: Arc<dyn Transport>,
    forward_timeout: Duration,
    storage_dir: Option<&Path>,
) -> Result<(Arc<dyn RequestHandler>, crate::NodeHandle<M>), StorageError>
where
    M: StateMachine + 'static,
{
    use craft_core::RaftNode;
    use craft_net::GroupTransport;

    use crate::{NodeService, RaftDriver, spawn_node};

    let driver = if let Some(dir) = storage_dir {
        let storage = GroupRedbLayout::new(dir).open_group(group)?;
        RaftDriver::recover(
            node_id,
            members.iter().copied(),
            raft,
            machine,
            Box::new(storage) as Box<dyn RaftStorage>,
        )
        .map_err(|e| StorageError::Backend(e.to_string()))?
    } else {
        let node = RaftNode::new(node_id, members.iter().copied(), raft);
        RaftDriver::new(node, machine)
    };
    let group_transport = Arc::new(GroupTransport::new(group, network)) as Arc<dyn Transport>;
    let handle = spawn_node(driver, Arc::clone(&group_transport), runtime);
    let service = Arc::new(
        NodeService::new(handle.clone(), group_transport).with_forward_timeout(forward_timeout),
    ) as Arc<dyn RequestHandler>;
    Ok((service, handle))
}

/// Spawn `group_count` independent Raft groups on one physical node, wired
/// through a [`ShardedNodeService`]. Each group's bootstrap voters are chosen
/// by [`group_voters`](craft_core::group_voters) over `live_nodes` (per-group-raft-membership).
#[allow(clippy::too_many_arguments)]
pub fn spawn_multi_raft_node<M>(
    node_id: NodeId,
    live_nodes: &[NodeId],
    replication_factor: u32,
    raft: craft_core::Config,
    runtime: RuntimeConfig,
    runtime_group0: RuntimeConfig,
    shard_count: u32,
    shard_routing: ShardRoutingKind,
    group_count: u32,
    machines: Vec<M>,
    network: Arc<dyn Transport>,
    forward_timeout: Duration,
    storage_dir: Option<&Path>,
) -> Result<(Arc<ShardedNodeService>, Vec<crate::NodeHandle<M>>), StorageError>
where
    M: StateMachine + 'static,
{
    use craft_core::group_voters;

    let group_ids: Vec<RaftGroupId> = (0..group_count).map(RaftGroupId).collect();
    let mut handles = Vec::new();
    let mut services = BTreeMap::new();

    for (g, machine) in machines.into_iter().enumerate().take(group_count as usize) {
        let g = g as u32;
        let voters = group_voters(RaftGroupId(g), live_nodes, replication_factor);
        let rt = if g == 0 {
            runtime_group0.clone()
        } else {
            runtime.clone()
        };
        let (service, handle) = spawn_raft_group(
            node_id,
            &voters,
            g,
            raft.clone(),
            rt,
            machine,
            Arc::clone(&network),
            forward_timeout,
            storage_dir,
        )?;
        services.insert(g, service);
        handles.push(handle);
    }

    let sharded = Arc::new(ShardedNodeService::new(
        shard_count,
        shard_routing,
        group_ids,
        services,
    ));
    Ok((sharded, handles))
}
