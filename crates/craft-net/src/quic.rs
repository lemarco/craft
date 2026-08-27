//! Live HTTP/3-over-QUIC server and client (ADR 010, backlog C2).
//!
//! [`QuicServer`] runs the mTLS accept loop, turning each authenticated QUIC
//! connection into an `h3` server connection and dispatching `/raft/v1/*`
//! requests to a [`RequestHandler`]. [`QuicTransport`] implements the
//! [`Transport`] port over an `h3` client, caching one connection per peer so
//! consensus RPCs reuse the QUIC handshake (a minimal pool; C5 adds
//! reconnect/backoff).

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use bytes::{Buf, Bytes};
use craft_proto::NodeId;
use http::{Request, Response, StatusCode};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::backoff::{BackoffPolicy, BackoffState};
use crate::peer::PeerDirectory;
use crate::priority::TrafficPolicy;
use crate::route::{Route, TrafficClass};
use crate::tls::node_server_name;
use crate::transport::{Body, BoxFuture, RequestHandler, Transport, TransportError};
use crate::wire::MAX_BODY_BYTES;

fn io<E: fmt::Display>(e: E) -> TransportError {
    TransportError::Io(e.to_string())
}

/// The HTTP/3 server: owns a QUIC endpoint and serves requests to a handler.
pub struct QuicServer {
    endpoint: quinn::Endpoint,
}

impl QuicServer {
    /// Bind a server endpoint on `addr` with the given mTLS server config
    /// (build it with [`crate::tls::server_config`]).
    ///
    /// # Errors
    /// Returns [`TransportError::Io`] if the socket cannot be bound.
    pub fn bind(addr: SocketAddr, config: quinn::ServerConfig) -> Result<Self, TransportError> {
        let endpoint = quinn::Endpoint::server(config, addr).map_err(io)?;
        Ok(Self { endpoint })
    }

    /// The address the server is actually listening on (useful with port `0`).
    ///
    /// # Errors
    /// Returns [`TransportError::Io`] if the local address cannot be read.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.endpoint.local_addr().map_err(io)
    }

    /// The underlying QUIC endpoint (e.g. to share it with a client transport).
    #[must_use]
    pub fn endpoint(&self) -> &quinn::Endpoint {
        &self.endpoint
    }

    /// Swap the server TLS configuration for **new** handshakes (ADR 034).
    ///
    /// Existing connections keep their original TLS session until they close.
    pub fn reload(&self, config: quinn::ServerConfig) {
        self.endpoint.set_server_config(Some(config));
    }

    /// Accept connections forever, dispatching each request to `handler`.
    /// Returns once the endpoint is closed.
    pub async fn run(self, handler: Arc<dyn RequestHandler>) {
        Self::accept_loop(&self.endpoint, handler).await;
    }

    /// Like [`run`](Self::run) but keeps the server in an [`Arc`] so other tasks
    /// (e.g. cert hot-reload, ADR 034) can call [`reload`](Self::reload).
    pub async fn run_arc(self: Arc<Self>, handler: Arc<dyn RequestHandler>) {
        Self::accept_loop(&self.endpoint, handler).await;
    }

    async fn accept_loop(endpoint: &quinn::Endpoint, handler: Arc<dyn RequestHandler>) {
        while let Some(incoming) = endpoint.accept().await {
            let handler = handler.clone();
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => serve_connection(conn, handler).await,
                    Err(_) => { /* handshake failed (e.g. bad client cert) */ }
                }
            });
        }
    }
}

async fn serve_connection(conn: quinn::Connection, handler: Arc<dyn RequestHandler>) {
    let mut h3 = match h3::server::Connection::new(h3_quinn::Connection::new(conn)).await {
        Ok(h3) => h3,
        Err(_) => return,
    };
    // Ends when `accept` yields `None` (graceful) or `Err` (connection closed).
    while let Ok(Some(resolver)) = h3.accept().await {
        let handler = handler.clone();
        tokio::spawn(async move {
            let _ = handle_request(resolver, handler).await;
        });
    }
}

async fn handle_request(
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
    handler: Arc<dyn RequestHandler>,
) -> Result<(), TransportError> {
    let (req, mut stream) = resolver.resolve_request().await.map_err(io)?;
    let route = Route::from_path(req.uri().path());

    // Drain the request body, enforcing the size cap.
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.map_err(io)? {
        let bytes = chunk.copy_to_bytes(chunk.remaining());
        body.extend_from_slice(&bytes);
        if body.len() > MAX_BODY_BYTES {
            let response = Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(())
                .map_err(io)?;
            stream.send_response(response).await.map_err(io)?;
            stream.finish().await.map_err(io)?;
            return Ok(());
        }
    }

    let (status, response_body) = match route {
        Some(route) => match handler.handle(route, body).await {
            Ok(body) => (StatusCode::OK, body),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Vec::new()),
        },
        None => (StatusCode::NOT_FOUND, Vec::new()),
    };

    let response = Response::builder().status(status).body(()).map_err(io)?;
    stream.send_response(response).await.map_err(io)?;
    if !response_body.is_empty() {
        stream
            .send_data(Bytes::from(response_body))
            .await
            .map_err(io)?;
    }
    stream.finish().await.map_err(io)?;
    Ok(())
}

/// The `h3` client `SendRequest` handle cached per peer connection.
type ClientSender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// Max concurrent in-flight requests (open QUIC streams) to one peer on the
/// [`TrafficClass::Actor`] connection. A burst of slow `ask`s queues on this
/// gate (backpressure) instead of opening unbounded streams and exhausting the
/// connection's stream limit — which would stall casts / spawns / directory
/// sync to that peer (ADR 027 R2). Sized below quinn's default bidi-stream cap
/// (100) to leave headroom.
const ACTOR_MAX_INFLIGHT: usize = 64;

/// The concurrent-stream ceiling for `class`, or `None` to leave it unbounded.
/// Consensus (`Peer`) is never gated, so heartbeats never queue behind bulk
/// traffic; short client/cluster round-trips are left unbounded for now.
fn class_stream_limit(class: TrafficClass) -> Option<usize> {
    match class {
        TrafficClass::Actor => Some(ACTOR_MAX_INFLIGHT),
        TrafficClass::Peer | TrafficClass::Client | TrafficClass::Cluster => None,
    }
}

/// A cached connection to one peer for one [`TrafficClass`], plus its reconnect
/// backoff (C5). Consensus (`Peer`) traffic gets its own entry, isolated from
/// bulk client/actor streams (ADR 027 R2).
#[derive(Default)]
struct PeerConn {
    sender: Option<ClientSender>,
    backoff: BackoffState,
    /// Bounds concurrent in-flight streams for gated classes (see
    /// [`class_stream_limit`]); `None` leaves the class unbounded.
    gate: Option<Arc<Semaphore>>,
}

impl PeerConn {
    /// A fresh entry for `class`, arming its stream gate when the class is
    /// bounded.
    fn for_class(class: TrafficClass) -> Self {
        Self {
            sender: None,
            backoff: BackoffState::default(),
            gate: class_stream_limit(class).map(|n| Arc::new(Semaphore::new(n))),
        }
    }
}

struct Inner {
    endpoint: quinn::Endpoint,
    client_config: RwLock<quinn::ClientConfig>,
    // Runtime-mutable so peers learned dynamically (a node joining via
    // `/cluster/join`, addresses gossiped over `/cluster/peers`) become dialable
    // without restarting the transport (ADR 007). Guarded by a std `RwLock`
    // held only for the brief address lookup/update, never across an `.await`.
    directory: RwLock<PeerDirectory>,
    policy: BackoffPolicy,
    // Per-traffic-class admission control so bulk client/actor sends cannot
    // starve Raft heartbeats on the shared socket (ADR 027 R2).
    traffic: TrafficPolicy,
    conns: Mutex<HashMap<(NodeId, TrafficClass), PeerConn>>,
}

/// A [`Transport`] over HTTP/3: dials peers by [`NodeId`] using the
/// [`PeerDirectory`], authenticates with mTLS, and performs one request/response
/// per [`send`](Transport::send). Connections are cached **per peer and traffic
/// class** and reused, so latency-sensitive consensus RPCs never share a QUIC
/// connection with bulk client/actor traffic (ADR 027 R2). Failed dials back
/// off exponentially ([`BackoffPolicy`]) so a dead peer is not hammered.
#[derive(Clone)]
pub struct QuicTransport {
    inner: Arc<Inner>,
}

impl QuicTransport {
    /// Build a client transport from a client QUIC endpoint (see
    /// [`client_endpoint`]), an mTLS client config (build it with
    /// [`crate::tls::client_config`]), and the peer address book, using the
    /// default reconnect [`BackoffPolicy`].
    #[must_use]
    pub fn new(
        endpoint: quinn::Endpoint,
        client_config: quinn::ClientConfig,
        directory: PeerDirectory,
    ) -> Self {
        Self::with_backoff(endpoint, client_config, directory, BackoffPolicy::default())
    }

    /// Like [`new`](QuicTransport::new) but with an explicit reconnect
    /// [`BackoffPolicy`].
    #[must_use]
    pub fn with_backoff(
        endpoint: quinn::Endpoint,
        client_config: quinn::ClientConfig,
        directory: PeerDirectory,
        policy: BackoffPolicy,
    ) -> Self {
        Self::with_policy(
            endpoint,
            client_config,
            directory,
            policy,
            TrafficPolicy::unlimited(),
        )
    }

    /// Like [`with_backoff`](QuicTransport::with_backoff) but with an explicit
    /// per-traffic-class [`TrafficPolicy`] (ADR 027 R2): rate-limit bulk
    /// client/actor traffic so latency-sensitive consensus RPCs are never
    /// starved on the shared UDP socket.
    #[must_use]
    pub fn with_policy(
        endpoint: quinn::Endpoint,
        client_config: quinn::ClientConfig,
        directory: PeerDirectory,
        policy: BackoffPolicy,
        traffic: TrafficPolicy,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                endpoint,
                client_config: RwLock::new(client_config),
                directory: RwLock::new(directory),
                policy,
                traffic,
                conns: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Learn (or update) a peer's address at runtime — e.g. when a node joins
    /// via `/cluster/join` or its address is gossiped over `/cluster/peers`
    /// (ADR 007). Subsequent dials to `id` use `addr`.
    pub fn learn_peer(&self, id: NodeId, addr: SocketAddr) {
        self.inner
            .directory
            .write()
            .expect("peer directory poisoned")
            .insert(id, addr);
    }

    /// Forget a peer that has left the cluster; future dials to it fail fast.
    pub fn forget_peer(&self, id: NodeId) {
        self.inner
            .directory
            .write()
            .expect("peer directory poisoned")
            .remove(id);
    }

    /// A snapshot of the currently known peer addresses.
    #[must_use]
    pub fn peers(&self) -> PeerDirectory {
        self.inner
            .directory
            .read()
            .expect("peer directory poisoned")
            .clone()
    }

    /// Swap the outbound client TLS config and drop cached connections so the
    /// next dial uses the new identity (ADR 034).
    pub async fn reload(&self, client_config: quinn::ClientConfig) {
        *self
            .inner
            .client_config
            .write()
            .expect("client config poisoned") = client_config;
        self.inner.conns.lock().await.clear();
    }
}

/// Acquire an in-flight-stream permit for `(peer, class)`, or `None` if the
/// class is unbounded. Awaits if the gate is saturated (backpressure). The gate
/// `Arc` is cloned under the connection lock, then acquired *without* holding
/// it, so a saturated gate never blocks other peers/classes.
async fn acquire_stream_permit(
    inner: &Inner,
    peer: NodeId,
    class: TrafficClass,
) -> Option<OwnedSemaphorePermit> {
    let gate = {
        let mut conns = inner.conns.lock().await;
        conns
            .entry((peer, class))
            .or_insert_with(|| PeerConn::for_class(class))
            .gate
            .clone()
    };
    match gate {
        // The semaphore is never closed, so acquisition cannot fail.
        Some(sem) => Some(sem.acquire_owned().await.expect("stream gate is open")),
        None => None,
    }
}

/// Get a cached sender for `(peer, class)` or dial a fresh connection. A dial
/// that is still inside its backoff window short-circuits to
/// [`TransportError::Unreachable`] without touching the socket; a dial failure
/// arms the backoff, and a success clears it.
async fn connect_sender(
    inner: &Inner,
    peer: NodeId,
    class: TrafficClass,
) -> Result<ClientSender, TransportError> {
    let mut conns = inner.conns.lock().await;
    let entry = conns
        .entry((peer, class))
        .or_insert_with(|| PeerConn::for_class(class));
    if let Some(sender) = &entry.sender {
        return Ok(sender.clone());
    }
    if !entry.backoff.ready(Instant::now()) {
        // Still backing off from a recent failure — do not redial yet.
        return Err(TransportError::Unreachable(peer));
    }

    let dialed = dial(inner, peer).await;
    match dialed {
        Ok(sender) => {
            entry.sender = Some(sender.clone());
            entry.backoff.reset();
            Ok(sender)
        }
        Err(e) => {
            entry.backoff.record_failure(&inner.policy, Instant::now());
            Err(e)
        }
    }
}

/// Perform the actual mTLS QUIC + HTTP/3 handshake to `peer`.
async fn dial(inner: &Inner, peer: NodeId) -> Result<ClientSender, TransportError> {
    let addr = inner
        .directory
        .read()
        .expect("peer directory poisoned")
        .addr(peer)
        .ok_or(TransportError::Unreachable(peer))?;
    let server_name = node_server_name(peer);
    let client_cfg = inner
        .client_config
        .read()
        .expect("client config poisoned")
        .clone();
    let connecting = inner
        .endpoint
        .connect_with(client_cfg, addr, &server_name)
        .map_err(io)?;
    let conn = connecting.await.map_err(io)?;

    let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .map_err(io)?;
    // Drive the connection in the background until it closes.
    tokio::spawn(async move {
        let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    Ok(sender)
}

async fn round_trip(
    inner: Arc<Inner>,
    peer: NodeId,
    route: Route,
    body: Body,
) -> Result<Body, TransportError> {
    let class = route.traffic_class();
    // Admission control: throttle rate-limited classes before touching the
    // socket, keeping consensus (unthrottled) ahead of bulk traffic (ADR 027 R2).
    inner.traffic.admit(class).await;
    // Backpressure: cap concurrent in-flight streams on gated classes so a burst
    // of slow `ask`s queues here instead of exhausting the connection's stream
    // limit and stalling other Actor-class traffic to this peer. Held for the
    // whole round-trip; released when `_permit` drops.
    let _permit = acquire_stream_permit(&inner, peer, class).await;
    let mut sender = connect_sender(&inner, peer, class).await?;

    let request = Request::builder()
        .method(route.method())
        .uri(format!(
            "https://{}{}",
            node_server_name(peer),
            route.path()
        ))
        .body(())
        .map_err(io)?;

    let send = async {
        let mut stream = sender.send_request(request).await.map_err(io)?;
        stream.send_data(Bytes::from(body)).await.map_err(io)?;
        stream.finish().await.map_err(io)?;

        let _response = stream.recv_response().await.map_err(io)?;
        let mut out = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.map_err(io)? {
            let bytes = chunk.copy_to_bytes(chunk.remaining());
            out.extend_from_slice(&bytes);
            if out.len() > MAX_BODY_BYTES {
                return Err(TransportError::Wire(crate::wire::WireError::BodyTooLarge {
                    size: out.len(),
                }));
            }
        }
        Ok(out)
    }
    .await;

    // A send failure on an established connection drops it and arms the backoff,
    // so the next call redials (after the window) instead of reusing a dead one.
    if send.is_err() {
        let mut conns = inner.conns.lock().await;
        if let Some(entry) = conns.get_mut(&(peer, class)) {
            entry.sender = None;
            entry.backoff.record_failure(&inner.policy, Instant::now());
        }
    }
    send
}

impl Transport for QuicTransport {
    fn send(
        &self,
        peer: NodeId,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        let inner = self.inner.clone();
        Box::pin(round_trip(inner, peer, route, body))
    }
}

/// Create a client-only QUIC endpoint bound to an ephemeral local port.
///
/// # Errors
/// Returns [`TransportError::Io`] if the socket cannot be bound.
pub fn client_endpoint(bind: SocketAddr) -> Result<quinn::Endpoint, TransportError> {
    quinn::Endpoint::client(bind).map_err(io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_actor_traffic_is_stream_gated() {
        // Bulk/actor sends are bounded; consensus and short client/cluster
        // round-trips are not, so heartbeats never queue behind bulk streams.
        assert_eq!(
            class_stream_limit(TrafficClass::Actor),
            Some(ACTOR_MAX_INFLIGHT)
        );
        assert_eq!(class_stream_limit(TrafficClass::Peer), None);
        assert_eq!(class_stream_limit(TrafficClass::Client), None);
        assert_eq!(class_stream_limit(TrafficClass::Cluster), None);
    }

    #[test]
    fn an_actor_connection_arms_a_bounded_gate() {
        let actor = PeerConn::for_class(TrafficClass::Actor);
        let gate = actor.gate.expect("the actor class is gated");
        assert_eq!(gate.available_permits(), ACTOR_MAX_INFLIGHT);

        let peer = PeerConn::for_class(TrafficClass::Peer);
        assert!(peer.gate.is_none(), "consensus is never gated");
    }

    #[tokio::test]
    async fn a_saturated_gate_blocks_until_a_permit_is_released() {
        // A 1-permit gate makes saturation observable without opening 64 streams.
        let gate = Arc::new(Semaphore::new(1));
        let held = Arc::clone(&gate).acquire_owned().await.unwrap();
        assert_eq!(gate.available_permits(), 0);

        let waiter = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.acquire_owned().await.unwrap() }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "a further acquire blocks while the gate is saturated"
        );

        // Releasing the held permit lets the queued acquire through.
        drop(held);
        let _next = waiter.await.unwrap();
    }
}
