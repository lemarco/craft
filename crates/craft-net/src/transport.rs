//! The [`Transport`] port and an in-memory implementation (wire-transport, architecture-style).
//!
//! wire-transport requires that the deterministic simulator and the real `quinn`/`h3`
//! stack expose *the same* transport abstraction, so the runtime is written
//! once against a trait. That trait is [`Transport`] (the client side: send a
//! request to a peer, await the response) paired with [`RequestHandler`] (the
//! server side: turn an inbound request body into a response body).
//!
//! Both use boxed-future signatures rather than `async fn` in trait position so
//! they stay object-safe — the runtime can hold an `Arc<dyn Transport>` and swap
//! the [`LocalNetwork`] test double for a QUIC adapter with no code changes.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use craft_proto::{
    ActorEnvelope, ClientRequest, ClientResponse, DeliverAck, DirectoryUpdate, GroupMigrateReply,
    GroupMigrateRequest, JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, MigrateReply,
    MigrateRequest, NodeId, PeerBook, RaftRpc, RaftRpcReply, RegisterAck, ScaleReply, ScaleRequest,
    SpawnReply, SpawnRequest, StopReply, StopRequest,
};

use crate::route::Route;
use crate::wire::{WireError, decode_body, encode_body};

/// A boxed, `Send` future — the return type of the object-safe transport traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A response body produced by a [`RequestHandler`] or awaited from a
/// [`Transport::send`].
pub type Body = Vec<u8>;

/// An error sending a request or handling one.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The target peer is not reachable (unknown, disconnected, or partitioned).
    #[error("peer {0:?} is unreachable")]
    Unreachable(NodeId),

    /// A body could not be framed/unframed.
    #[error("wire: {0}")]
    Wire(#[from] WireError),

    /// A lower-level transport/IO failure (QUIC, timeout, etc.).
    #[error("transport io: {0}")]
    Io(String),
}

/// A failure interacting with a remote node during a cross-node operation
/// (spawn, cast, ask, scale, migrate, stop). Factors the two near-identical
/// outcomes that every such operation shares, so the domain error enums embed
/// one `Remote` arm instead of duplicating `Transport { node, reason }` /
/// `Rejected { node, reason }` pairs.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// The request could not be shipped to the target node (unreachable, dial
    /// failure, framing, timeout).
    #[error("transport to {node:?} failed: {reason}")]
    Transport {
        /// The target node.
        node: NodeId,
        /// The underlying transport error.
        reason: String,
    },
    /// The target node received the request but reported that it could not
    /// carry it out.
    #[error("node {node:?} rejected the request: {reason}")]
    Rejected {
        /// The node that rejected the request.
        node: NodeId,
        /// The reason it reported.
        reason: String,
    },
}

impl RemoteError {
    /// A shipping failure to `node`, capturing `source`'s display form.
    #[must_use]
    pub fn transport(node: NodeId, source: impl core::fmt::Display) -> Self {
        Self::Transport {
            node,
            reason: source.to_string(),
        }
    }

    /// A rejection reported by `node`.
    #[must_use]
    pub fn rejected(node: NodeId, reason: impl Into<String>) -> Self {
        Self::Rejected {
            node,
            reason: reason.into(),
        }
    }
}

/// Server side: handle an inbound request for `route` and produce a response
/// body. Implemented by the node runtime; the QUIC server and [`LocalNetwork`]
/// both call it.
pub trait RequestHandler: Send + Sync + 'static {
    /// Handle one request, returning the response body to send back.
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>>;
}

/// Client side: send a request body to `peer` on `route` and await the response
/// body. One call is one request/response round-trip (wire-transport).
pub trait Transport: Send + Sync + 'static {
    /// Send `body` to `peer` and await its response.
    fn send(
        &self,
        peer: NodeId,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>>;
}

// Blanket impls so `&T`/`Arc<T>` transports work transparently.
impl<T: Transport + ?Sized> Transport for Arc<T> {
    fn send(
        &self,
        peer: NodeId,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        (**self).send(peer, route, body)
    }
}

/// Send a typed Raft peer RPC and decode the typed reply (`/peer/wire`).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_peer_rpc<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    rpc: &RaftRpc,
) -> Result<RaftRpcReply, TransportError> {
    let body = encode_body(rpc)?;
    let response = transport.send(peer, Route::PeerWire, body).await?;
    Ok(decode_body(&response)?)
}

/// Send a typed client request and decode the typed response (`/client/wire`).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_client_request<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &ClientRequest,
) -> Result<ClientResponse, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ClientWire, body).await?;
    Ok(decode_body(&response)?)
}

/// Send a cluster [`JoinRequest`] and decode the [`JoinResponse`]
/// (`/cluster/join`, join-rpc).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_join_request<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &JoinRequest,
) -> Result<JoinResponse, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ClusterJoin, body).await?;
    Ok(decode_body(&response)?)
}

/// Send a cluster [`LeaveRequest`] and decode the [`LeaveResponse`]
/// (`/cluster/leave`).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_leave_request<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &LeaveRequest,
) -> Result<LeaveResponse, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ClusterLeave, body).await?;
    Ok(decode_body(&response)?)
}

/// Fetch a peer's [`PeerBook`] for address propagation (`/cluster/peers`,
/// discovery). The request body is empty; the response is the peer's current view
/// of reachable node addresses.
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn fetch_peers<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
) -> Result<PeerBook, TransportError> {
    let response = transport
        .send(peer, Route::ClusterPeers, Vec::new())
        .await?;
    Ok(decode_body(&response)?)
}

/// Publish a [`DirectoryUpdate`] to a peer and decode its [`RegisterAck`]
/// (`/actor/register`, cross-node-actors).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_directory_update<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    update: &DirectoryUpdate,
) -> Result<RegisterAck, TransportError> {
    let body = encode_body(update)?;
    let response = transport.send(peer, Route::ActorRegister, body).await?;
    Ok(decode_body(&response)?)
}

/// Deliver an [`ActorEnvelope`] to the node hosting the target instance and
/// decode its [`DeliverAck`] (`/actor/deliver`, cross-node-actors).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_actor_deliver<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    envelope: &ActorEnvelope,
) -> Result<DeliverAck, TransportError> {
    let body = encode_body(envelope)?;
    let response = transport.send(peer, Route::ActorDeliver, body).await?;
    Ok(decode_body(&response)?)
}

/// Ask a peer to spawn an actor and decode its [`SpawnReply`]
/// (`/actor/spawn`, cross-node-actors).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_actor_spawn<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &SpawnRequest,
) -> Result<SpawnReply, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ActorSpawn, body).await?;
    Ok(decode_body(&response)?)
}

/// Forward a cluster-wide scale to the (leader) peer and decode its
/// [`ScaleReply`] (`/actor/scale`, cross-node-actors, supervisor-leader).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_actor_scale<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &ScaleRequest,
) -> Result<ScaleReply, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ActorScale, body).await?;
    Ok(decode_body(&response)?)
}

/// Ask a peer to spawn a migration replacement and decode its [`MigrateReply`]
/// (`/actor/migrate`, cross-node-actors).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_actor_migrate<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &MigrateRequest,
) -> Result<MigrateReply, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ActorMigrate, body).await?;
    Ok(decode_body(&response)?)
}

/// Ship a Raft group migration bundle to `peer` and decode its
/// [`GroupMigrateReply`] (`/cluster/group/migrate`, write-sharding-multi-raft).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_group_migrate<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &GroupMigrateRequest,
) -> Result<GroupMigrateReply, TransportError> {
    let body = encode_body(request)?;
    let response = transport
        .send(peer, Route::ClusterGroupMigrate, body)
        .await?;
    Ok(decode_body(&response)?)
}

/// Ask a peer to stop a group for a planned scale-down / removal and decode its
/// [`StopReply`] (`/actor/stop`, cross-node-actors, supervisor-leader).
///
/// # Errors
/// Returns [`TransportError`] on a framing failure or if the peer is
/// unreachable / the send fails.
pub async fn send_actor_stop<T: Transport + ?Sized>(
    transport: &T,
    peer: NodeId,
    request: &StopRequest,
) -> Result<StopReply, TransportError> {
    let body = encode_body(request)?;
    let response = transport.send(peer, Route::ActorStop, body).await?;
    Ok(decode_body(&response)?)
}

/// An in-memory switch that wires several nodes' [`RequestHandler`]s together
/// with no real network — the deterministic test/simulation transport (wire-transport
/// mitigations). Cloning shares the same switch, so every node uses one handle.
///
/// [`detach`](LocalNetwork::detach) drops a node from the switch, which models a
/// crash or partition: sends to it then fail with [`TransportError::Unreachable`].
#[derive(Clone, Default)]
pub struct LocalNetwork {
    nodes: Arc<Mutex<HashMap<NodeId, Arc<dyn RequestHandler>>>>,
}

impl LocalNetwork {
    /// An empty network.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the handler serving requests addressed to `id`.
    pub fn attach(&self, id: NodeId, handler: Arc<dyn RequestHandler>) {
        self.nodes.lock().expect("poisoned").insert(id, handler);
    }

    /// Remove a node from the switch (crash/partition). Returns whether it was
    /// present.
    pub fn detach(&self, id: NodeId) -> bool {
        self.nodes.lock().expect("poisoned").remove(&id).is_some()
    }

    /// Whether `id` is currently reachable on the switch.
    #[must_use]
    pub fn is_reachable(&self, id: NodeId) -> bool {
        self.nodes.lock().expect("poisoned").contains_key(&id)
    }
}

impl fmt::Debug for LocalNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.nodes.lock().map(|n| n.len()).unwrap_or(0);
        f.debug_struct("LocalNetwork")
            .field("attached_nodes", &count)
            .finish()
    }
}

impl Transport for LocalNetwork {
    fn send(
        &self,
        peer: NodeId,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        // Clone the target handler out under the lock, then release it before
        // awaiting so a handler can itself call back into the network.
        let handler = self.nodes.lock().expect("poisoned").get(&peer).cloned();
        Box::pin(async move {
            match handler {
                Some(handler) => handler.handle(route, body).await,
                None => Err(TransportError::Unreachable(peer)),
            }
        })
    }
}
