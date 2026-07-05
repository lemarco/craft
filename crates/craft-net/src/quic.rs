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
use std::sync::Arc;

use bytes::{Buf, Bytes};
use craft_proto::NodeId;
use http::{Request, Response, StatusCode};
use tokio::sync::Mutex;

use crate::peer::PeerDirectory;
use crate::route::Route;
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

    /// Accept connections forever, dispatching each request to `handler`.
    /// Returns once the endpoint is closed.
    pub async fn run(self, handler: Arc<dyn RequestHandler>) {
        while let Some(incoming) = self.endpoint.accept().await {
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

/// The `h3` client `SendRequest` handle cached per peer.
type ClientSender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

struct Inner {
    endpoint: quinn::Endpoint,
    client_config: quinn::ClientConfig,
    directory: PeerDirectory,
    senders: Mutex<HashMap<NodeId, ClientSender>>,
}

/// A [`Transport`] over HTTP/3: dials peers by [`NodeId`] using the
/// [`PeerDirectory`], authenticates with mTLS, and performs one request/response
/// per [`send`](Transport::send). Connections are cached per peer and reused.
#[derive(Clone)]
pub struct QuicTransport {
    inner: Arc<Inner>,
}

impl QuicTransport {
    /// Build a client transport from a client QUIC endpoint (see
    /// [`client_endpoint`]), an mTLS client config (build it with
    /// [`crate::tls::client_config`]), and the peer address book.
    #[must_use]
    pub fn new(
        endpoint: quinn::Endpoint,
        client_config: quinn::ClientConfig,
        directory: PeerDirectory,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                endpoint,
                client_config,
                directory,
                senders: Mutex::new(HashMap::new()),
            }),
        }
    }
}

async fn connect_sender(inner: &Inner, peer: NodeId) -> Result<ClientSender, TransportError> {
    let mut senders = inner.senders.lock().await;
    if let Some(sender) = senders.get(&peer) {
        return Ok(sender.clone());
    }

    let addr = inner
        .directory
        .addr(peer)
        .ok_or(TransportError::Unreachable(peer))?;
    let server_name = node_server_name(peer);
    let connecting = inner
        .endpoint
        .connect_with(inner.client_config.clone(), addr, &server_name)
        .map_err(io)?;
    let conn = connecting.await.map_err(io)?;

    let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .map_err(io)?;
    // Drive the connection in the background until it closes.
    tokio::spawn(async move {
        let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    senders.insert(peer, sender.clone());
    Ok(sender)
}

async fn round_trip(
    inner: Arc<Inner>,
    peer: NodeId,
    route: Route,
    body: Body,
) -> Result<Body, TransportError> {
    let mut sender = connect_sender(&inner, peer).await?;

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

    // On failure, drop the cached connection so the next call reconnects.
    if send.is_err() {
        inner.senders.lock().await.remove(&peer);
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
