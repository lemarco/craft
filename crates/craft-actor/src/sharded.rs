//! Multi-Raft node routing — shard-aware client dispatch and group-scoped peer
//! RPC demux (ADR 031).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use craft_core::{RaftGroupId, ShardRouter, StateMachine, place_shard};
use craft_net::transport::{Body, BoxFuture, RequestHandler};
use craft_net::{Route, Transport, TransportError, decode_body, encode_body};
use craft_proto::{ClientRequest, ClientResponse, GroupPeerEnvelope, NodeId};

use crate::RuntimeConfig;

/// Routes client and peer traffic to one of several Raft groups hosted on the
/// same physical node.
pub struct ShardedNodeService {
    router: ShardRouter,
    group_ids: Vec<RaftGroupId>,
    groups: BTreeMap<u32, Arc<dyn RequestHandler>>,
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
            group_ids,
            groups,
        }
    }

    fn group_for_key(&self, key: &[u8]) -> Option<RaftGroupId> {
        let shard = self.router.shard_for(key);
        place_shard(shard, &self.group_ids)
    }

    fn route_client(&self, request: ClientRequest) -> Result<(u32, ClientRequest), ClientResponse> {
        match request {
            ClientRequest::Propose(bytes) => Ok((
                self.group_ids.first().map(|g| g.0).unwrap_or(0),
                ClientRequest::Propose(bytes),
            )),
            ClientRequest::Query(bytes) => Ok((
                self.group_ids.first().map(|g| g.0).unwrap_or(0),
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
                self.group_ids.first().map(|g| g.0).unwrap_or(0),
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
                let groups = self.groups.clone();
                Box::pin(async move {
                    let envelope: GroupPeerEnvelope = decode_body(&body)?;
                    let Some(handler) = groups.get(&envelope.group) else {
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
                let groups = self.groups.clone();
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
                    let Some(handler) = groups.get(&group_id) else {
                        return Err(TransportError::Io(format!(
                            "unknown raft group {group_id}"
                        )));
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
                let handler = self.groups.values().next().cloned();
                Box::pin(async move {
                    let Some(handler) = handler else {
                        return Err(TransportError::Io("no raft groups".into()));
                    };
                    handler.handle(other, body).await
                })
            }
        }
    }
}

/// Spawn `group_count` independent Raft groups on one physical node (same
/// member set), wired through a [`ShardedNodeService`].
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
) -> (ShardedNodeService, Vec<crate::NodeHandle<M>>)
where
    M: StateMachine + 'static,
{
    use craft_core::RaftNode;
    use craft_net::GroupTransport;

    use crate::{NodeService, RaftDriver, spawn_node};

    let group_ids: Vec<RaftGroupId> = (0..group_count).map(RaftGroupId).collect();
    let mut handles = Vec::new();
    let mut services = BTreeMap::new();

    for (g, machine) in machines.into_iter().enumerate().take(group_count as usize) {
        let g = g as u32;
        let node = RaftNode::new(node_id, members.iter().copied(), raft.clone());
        let driver = RaftDriver::new(node, machine);
        let group_transport =
            Arc::new(GroupTransport::new(g, Arc::clone(&network))) as Arc<dyn Transport>;
        let handle = spawn_node(
            driver,
            Arc::clone(&group_transport),
            runtime.clone(),
        );
        let service = Arc::new(
            NodeService::new(handle.clone(), group_transport).with_forward_timeout(forward_timeout),
        ) as Arc<dyn RequestHandler>;
        services.insert(g, service);
        handles.push(handle);
    }

    let sharded = ShardedNodeService::new(shard_count, group_ids, services);
    (sharded, handles)
}
