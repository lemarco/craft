use std::sync::Arc;
use std::time::Duration;

use trembita_core::{Command as _, StateMachine};
use trembita_net::transport::{Body, BoxFuture};
use trembita_net::{
    RequestHandler, Route, Transport, TransportError, decode_body, encode_body,
    send_catalog_add_request, send_client_request, send_join_request, send_leave_request,
};
use trembita_proto::{
    CatalogAddRequest, CatalogAddResponse, CatalogRejection, ClientRequest, ClientResponse,
    ClientWireError, JoinRejection, JoinRequest, JoinResponse, LeaveRejection, LeaveRequest,
    LeaveResponse, NodeId, RaftRpc,
};

use super::handle::NodeHandle;
use super::types::ClientError;
use super::wire::{encode_client_ok, rpc_sender, runtime_error_to_wire};

/// A [`trembita_net`] [`RequestHandler`] that bridges inbound `/peer/wire` and
/// `/client/wire` requests into a running node via its [`NodeHandle`].
///
/// Attach it to a `QuicServer` (or `LocalNetwork`) so remote peers and clients
/// can reach the node. Client requests use **transparent forwarding** (client-routing):
/// a non-leader proxies the request to the current leader over the same
/// `transport` and returns the leader's response, so clients can connect to any
/// node without leader discovery. If no leader is known the request fails with
/// a [`ClientResponse::Err`] (`ClientWireError::NoLeaderElected`); forward attempts are bounded by
/// `forward_timeout` (elections converge quickly, so stale-hint hops are rare
/// and time-bounded rather than looping).
pub struct NodeService<M: StateMachine> {
    handle: NodeHandle<M>,
    transport: Arc<dyn Transport>,
    forward_timeout: Duration,
}

impl<M: StateMachine> NodeService<M> {
    /// Wrap a node handle as a request handler. `transport` is used to forward
    /// client requests to the leader when this node is a follower (client-routing);
    /// pass the same transport the node runtime uses.
    #[must_use]
    pub fn new(handle: NodeHandle<M>, transport: Arc<dyn Transport>) -> Self {
        Self {
            handle,
            transport,
            forward_timeout: Duration::from_secs(5),
        }
    }

    /// Override the per-forward deadline used when proxying to the leader.
    #[must_use]
    pub fn with_forward_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout = timeout;
        self
    }
}

impl<M: StateMachine> RequestHandler for NodeService<M> {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        let handle = self.handle.clone();
        let transport = Arc::clone(&self.transport);
        let forward_timeout = self.forward_timeout;
        Box::pin(async move {
            match route {
                Route::PeerWire => {
                    let rpc: RaftRpc = decode_body(&body)?;
                    let from = rpc_sender(&rpc);
                    let reply = handle
                        .deliver_rpc(from, rpc)
                        .await
                        .map_err(|e| TransportError::Io(e.to_string()))?;
                    Ok(encode_body(&reply)?)
                }
                Route::ClientWire => {
                    let request: ClientRequest = decode_body(&body)?;
                    let response =
                        route_client(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                Route::ClusterJoin => {
                    let request: JoinRequest = decode_body(&body)?;
                    let response = route_join(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                Route::ClusterLeave => {
                    let request: LeaveRequest = decode_body(&body)?;
                    let response = route_leave(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                Route::ClusterCatalogAdd => {
                    let request: CatalogAddRequest = decode_body(&body)?;
                    let response =
                        route_catalog_add(&handle, &transport, forward_timeout, request).await;
                    Ok(encode_body(&response)?)
                }
                other => Err(TransportError::Io(format!(
                    "route {other:?} is not served by the node runtime"
                ))),
            }
        })
    }
}

/// Serve a client request, using follower reads for queries (read-consistency) and
/// transparent forwarding for writes (client-routing).
async fn route_client<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: ClientRequest,
) -> ClientResponse {
    match request {
        ClientRequest::Query(bytes) => {
            route_query(handle, transport, forward_timeout, bytes, None).await
        }
        ClientRequest::QueryKeyed { key, query } => {
            route_query(handle, transport, forward_timeout, query, Some(key)).await
        }
        ClientRequest::TwoPhasePrepare {
            tx_id,
            key,
            command,
        } => route_two_phase_prepare(handle, transport, forward_timeout, tx_id, key, command).await,
        ClientRequest::TwoPhaseCommit { tx_id, key } => {
            route_two_phase_commit(handle, transport, forward_timeout, tx_id, key).await
        }
        ClientRequest::TwoPhaseAbort { tx_id, key } => {
            route_two_phase_abort(handle, transport, forward_timeout, tx_id, key).await
        }
        other => route_write_client(handle, transport, forward_timeout, other).await,
    }
}

async fn route_two_phase_prepare<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    tx_id: Vec<u8>,
    route_key: Vec<u8>,
    command: Vec<u8>,
) -> ClientResponse {
    match handle
        .two_phase_prepare(tx_id.clone(), route_key.clone(), command.clone())
        .await
    {
        Ok(()) => ClientResponse::Ok(Vec::new()),
        Err(ClientError::NotLeader {
            leader: Some(leader),
        }) if leader != handle.id() => {
            forward_to_leader(
                transport,
                forward_timeout,
                leader,
                ClientRequest::TwoPhasePrepare {
                    tx_id,
                    key: route_key,
                    command,
                },
            )
            .await
        }
        Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
        Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
    }
}

async fn route_two_phase_commit<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    tx_id: Vec<u8>,
    route_key: Vec<u8>,
) -> ClientResponse {
    match handle
        .two_phase_commit(tx_id.clone(), route_key.clone())
        .await
    {
        Ok(response) => encode_client_ok(&response),
        Err(ClientError::NotLeader {
            leader: Some(leader),
        }) if leader != handle.id() => {
            forward_to_leader(
                transport,
                forward_timeout,
                leader,
                ClientRequest::TwoPhaseCommit {
                    tx_id,
                    key: route_key,
                },
            )
            .await
        }
        Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
        Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
    }
}

async fn route_two_phase_abort<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    tx_id: Vec<u8>,
    route_key: Vec<u8>,
) -> ClientResponse {
    match handle
        .two_phase_abort(tx_id.clone(), route_key.clone())
        .await
    {
        Ok(()) => ClientResponse::Ok(Vec::new()),
        Err(ClientError::NotLeader {
            leader: Some(leader),
        }) if leader != handle.id() => {
            forward_to_leader(
                transport,
                forward_timeout,
                leader,
                ClientRequest::TwoPhaseAbort {
                    tx_id,
                    key: route_key,
                },
            )
            .await
        }
        Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
        Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
    }
}

/// Route a linearizable read: leader serves locally; followers confirm with
/// the leader then answer from local state (etcd-style follower read).
async fn route_query<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    bytes: Vec<u8>,
    route_key: Option<Vec<u8>>,
) -> ClientResponse {
    let query = match <M::Query as trembita_core::Query>::from_bytes(&bytes) {
        Ok(q) => q,
        Err(e) => return ClientResponse::Err(ClientWireError::DecodeQuery(e.to_string())),
    };
    match handle.query(query).await {
        Ok(response) => encode_client_ok(&response),
        Err(ClientError::NotLeader {
            leader: Some(leader),
        }) if leader != handle.id() => {
            match handle
                .follower_query_bytes(bytes, route_key, leader, transport, forward_timeout)
                .await
            {
                Ok(response) => encode_client_ok(&response),
                Err(e) => ClientResponse::Err(ClientWireError::FollowerRead(e.to_string())),
            }
        }
        Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
        Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
    }
}

/// Proposals (and keyed writes) still forward to the leader when needed.
async fn route_write_client<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: ClientRequest,
) -> ClientResponse {
    let local = serve_locally(handle, request.clone()).await;
    let ClientResponse::NotLeader { leader } = local else {
        return local;
    };
    match leader {
        Some(leader) if leader != handle.id() => {
            forward_to_leader(transport, forward_timeout, leader, request).await
        }
        _ => ClientResponse::Err(ClientWireError::NoLeaderElected),
    }
}

/// Proxy a client request to `leader`, bounded by `timeout`.
async fn forward_to_leader(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: ClientRequest,
) -> ClientResponse {
    match tokio::time::timeout(timeout, send_client_request(&**transport, leader, &request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => ClientResponse::Err(ClientWireError::ForwardFailed {
            leader,
            reason: e.to_string(),
        }),
        Err(_) => ClientResponse::Err(ClientWireError::ForwardTimeout { leader }),
    }
}

/// Serve a cluster join, forwarding to the leader if this node is a follower
/// (join-rpc step 2, same transparent pattern as client requests).
async fn route_join<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: JoinRequest,
) -> JoinResponse {
    let local = handle
        .join(request.clone())
        .await
        .unwrap_or_else(|_| JoinResponse::Rejected {
            reason: JoinRejection::Other("node runtime stopped".to_string()),
        });
    // A follower that knows the leader redirects; forward there on the caller's
    // behalf so a joining node only needs one seed address.
    if let JoinResponse::Redirect {
        leader: Some(leader),
    } = local
        && leader != handle.id()
    {
        return forward_join(transport, forward_timeout, leader, request).await;
    }
    local
}

/// Serve a cluster leave, forwarding to the leader if this node is a follower.
async fn route_leave<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: LeaveRequest,
) -> LeaveResponse {
    let local = handle
        .leave(request.clone())
        .await
        .unwrap_or_else(|_| LeaveResponse::Rejected {
            reason: LeaveRejection::Other("node runtime stopped".to_string()),
        });
    if let LeaveResponse::Redirect {
        leader: Some(leader),
    } = local
        && leader != handle.id()
    {
        return forward_leave(transport, forward_timeout, leader, request).await;
    }
    local
}

/// Serve a catalog add, forwarding to the group 0 leader if this node is a follower.
async fn route_catalog_add<M: StateMachine>(
    handle: &NodeHandle<M>,
    transport: &Arc<dyn Transport>,
    forward_timeout: Duration,
    request: CatalogAddRequest,
) -> CatalogAddResponse {
    let local = handle
        .catalog_add(request.clone())
        .await
        .unwrap_or_else(|_| CatalogAddResponse::Rejected {
            reason: CatalogRejection::Other("node runtime stopped".to_string()),
        });
    if let CatalogAddResponse::Redirect {
        leader: Some(leader),
    } = local
        && leader != handle.id()
    {
        return forward_catalog_add(transport, forward_timeout, leader, request).await;
    }
    local
}

/// Proxy a catalog add request to `leader`, bounded by `timeout`.
async fn forward_catalog_add(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: CatalogAddRequest,
) -> CatalogAddResponse {
    match tokio::time::timeout(
        timeout,
        send_catalog_add_request(&**transport, leader, &request),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => CatalogAddResponse::Redirect {
            leader: Some(leader),
        },
    }
}

/// Proxy a join request to `leader`, bounded by `timeout`.
async fn forward_join(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: JoinRequest,
) -> JoinResponse {
    match tokio::time::timeout(timeout, send_join_request(&**transport, leader, &request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => JoinResponse::Redirect {
            leader: Some(leader),
        },
    }
}

/// Proxy a leave request to `leader`, bounded by `timeout`.
async fn forward_leave(
    transport: &Arc<dyn Transport>,
    timeout: Duration,
    leader: NodeId,
    request: LeaveRequest,
) -> LeaveResponse {
    match tokio::time::timeout(timeout, send_leave_request(&**transport, leader, &request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => LeaveResponse::Redirect {
            leader: Some(leader),
        },
    }
}

/// Answer a decoded [`ClientRequest`] against the local node, mapping runtime
/// results onto the wire [`ClientResponse`] (no forwarding).
async fn serve_locally<M: StateMachine>(
    handle: &NodeHandle<M>,
    request: ClientRequest,
) -> ClientResponse {
    match request {
        ClientRequest::Propose(bytes) | ClientRequest::ProposeKeyed { command: bytes, .. } => {
            let command = match M::Command::from_bytes(&bytes) {
                Ok(c) => c,
                Err(e) => {
                    return ClientResponse::Err(ClientWireError::DecodeCommand(e.to_string()));
                }
            };
            match handle.propose(command).await {
                Ok(response) => encode_client_ok(&response),
                Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
                Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
            }
        }
        ClientRequest::Query(bytes) | ClientRequest::QueryKeyed { query: bytes, .. } => {
            let query = match <M::Query as trembita_core::Query>::from_bytes(&bytes) {
                Ok(q) => q,
                Err(e) => return ClientResponse::Err(ClientWireError::DecodeQuery(e.to_string())),
            };
            match handle.query(query).await {
                Ok(response) => encode_client_ok(&response),
                Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
                Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
            }
        }
        ClientRequest::ReadIndexConfirm { .. } => match handle.confirm_read_index().await {
            Ok((index, term)) => ClientResponse::ReadIndexConfirmed { index, term },
            Err(ClientError::NotLeader { leader }) => ClientResponse::NotLeader { leader },
            Err(e) => ClientResponse::Err(runtime_error_to_wire(e)),
        },
        ClientRequest::TwoPhasePrepare { .. }
        | ClientRequest::TwoPhaseCommit { .. }
        | ClientRequest::TwoPhaseAbort { .. } => {
            ClientResponse::Err(ClientWireError::TwoPhaseMisrouted)
        }
    }
}
