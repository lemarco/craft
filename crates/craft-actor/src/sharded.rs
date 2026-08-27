//! Multi-Raft node routing — shard-aware client dispatch and group-scoped peer
//! RPC demux (ADR 031).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use craft_core::{RaftGroupId, ShardRouter, StateMachine, place_shard};
use craft_net::transport::{Body, BoxFuture, RequestHandler};
use craft_net::{Route, Transport, TransportError, decode_body, encode_body};
use craft_proto::{ClientRequest, ClientResponse, GroupPeerEnvelope, NodeId};
use craft_storage::{GroupRedbLayout, RaftStorage, StorageError};

use crate::RuntimeConfig;

/// Routes client and peer traffic to one of several Raft groups hosted on the
/// same physical node.
pub struct ShardedNodeService {
    router: ShardRouter,
    group_ids: Arc<RwLock<Vec<RaftGroupId>>>,
    groups: Arc<RwLock<BTreeMap<u32, Arc<dyn RequestHandler>>>>,
}

impl ShardedNodeService {
    /// Build a sharded handler from per-group services.
    #[must_use]
    pub fn new(
        shard_count: u32,
        group_ids: Vec<RaftGroupId>,
        groups: BTreeMap<u32, Arc<dyn RequestHandler>>,
    ) -> Self {
        Self {
            router: ShardRouter::new(shard_count),
            group_ids: Arc::new(RwLock::new(group_ids)),
            groups: Arc::new(RwLock::new(groups)),
        }
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

    fn group_for_key(&self, key: &[u8]) -> Option<RaftGroupId> {
        let shard = self.router.shard_for(key);
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
                    return Err(ClientResponse::Error("no raft groups configured".into()));
                };
                Ok((group.0, ClientRequest::Propose(command)))
            }
            ClientRequest::QueryKeyed { key, query } => {
                let Some(group) = self.group_for_key(&key) else {
                    return Err(ClientResponse::Error("no raft groups configured".into()));
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
                    return Err(ClientResponse::Error("no raft groups configured".into()));
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

    let node = RaftNode::new(node_id, members.iter().copied(), raft);
    let driver = if let Some(dir) = storage_dir {
        let storage = GroupRedbLayout::new(dir).open_group(group)?;
        RaftDriver::with_storage(node, machine, Box::new(storage) as Box<dyn RaftStorage>)
    } else {
        RaftDriver::new(node, machine)
    };
    let group_transport = Arc::new(GroupTransport::new(group, network)) as Arc<dyn Transport>;
    let handle = spawn_node(driver, Arc::clone(&group_transport), runtime);
    let service = Arc::new(
        NodeService::new(handle.clone(), group_transport).with_forward_timeout(forward_timeout),
    ) as Arc<dyn RequestHandler>;
    Ok((service, handle))
}

/// Spawn `group_count` independent Raft groups on one physical node (same
/// member set), wired through a [`ShardedNodeService`].
#[allow(clippy::too_many_arguments)]
pub fn spawn_multi_raft_node<M>(
    node_id: NodeId,
    members: &[NodeId],
    raft: craft_core::Config,
    runtime: RuntimeConfig,
    shard_count: u32,
    group_count: u32,
    machines: Vec<M>,
    network: Arc<dyn Transport>,
    forward_timeout: Duration,
    storage_dir: Option<&Path>,
) -> Result<(Arc<ShardedNodeService>, Vec<crate::NodeHandle<M>>), StorageError>
where
    M: StateMachine + 'static,
{
    let group_ids: Vec<RaftGroupId> = (0..group_count).map(RaftGroupId).collect();
    let mut handles = Vec::new();
    let mut services = BTreeMap::new();

    for (g, machine) in machines.into_iter().enumerate().take(group_count as usize) {
        let g = g as u32;
        let (service, handle) = spawn_raft_group(
            node_id,
            members,
            g,
            raft.clone(),
            runtime.clone(),
            machine,
            Arc::clone(&network),
            forward_timeout,
            storage_dir,
        )?;
        services.insert(g, service);
        handles.push(handle);
    }

    let sharded = Arc::new(ShardedNodeService::new(shard_count, group_ids, services));
    Ok((sharded, handles))
}
