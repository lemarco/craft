//! The admin HTTP/1.1 server (health-admin-port) exposing health, readiness, Prometheus
//! metrics, introspection JSON, and the live dashboard + SSE feed (observability).
//!
//! Plain HTTP/1.1 on a **separate** port from the mTLS trembita wire, so ordinary
//! probes (`curl`, load balancers) work without QUIC or client certs. It
//! carries no consensus/client data — only read-only observability — and should
//! be bound to a private interface or firewalled (health-admin-port security notes).

use std::convert::Infallible;
use std::sync::Arc;

use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;

use crate::dashboard::DASHBOARD_HTML;
use crate::metrics::Metrics;
use crate::telemetry::EventBus;
use crate::views::Observer;

/// The response body type: either a buffered `Full` or a streamed SSE body,
/// unified behind `BoxBody`.
type Body = BoxBody<Bytes, Infallible>;

/// The admin/observability HTTP server.
///
/// Reads all data through an [`Observer`], a [`Metrics`] registry, and an
/// [`EventBus`]; owns none of the runtime.
pub struct AdminServer {
    observer: Arc<dyn Observer>,
    metrics: Metrics,
    events: EventBus,
}

impl AdminServer {
    /// Build a server over the given observability sources.
    #[must_use]
    pub fn new(observer: Arc<dyn Observer>, metrics: Metrics, events: EventBus) -> Self {
        Self {
            observer,
            metrics,
            events,
        }
    }

    /// Serve admin requests on `listener` until it errors. Each connection is
    /// handled on its own task; SSE connections stay open indefinitely.
    ///
    /// # Errors
    /// Returns the first [`std::io::Error`] from `accept`.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        self.serve_inner(listener, None).await
    }

    /// Serve admin requests over **TLS** (server-only, no client certificates).
    ///
    /// # Errors
    /// Returns the first accept or TLS handshake error.
    pub async fn serve_tls(
        self,
        listener: TcpListener,
        tls: Arc<ServerConfig>,
    ) -> std::io::Result<()> {
        self.serve_inner(listener, Some(tls)).await
    }

    async fn serve_inner(
        self,
        listener: TcpListener,
        tls: Option<Arc<ServerConfig>>,
    ) -> std::io::Result<()> {
        let state = Arc::new(self);
        let acceptor = tls.map(TlsAcceptor::from);
        loop {
            let (stream, _peer) = listener.accept().await?;
            let state = Arc::clone(&state);
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Some(acc) = acceptor {
                    if let Ok(s) = acc.accept(stream).await {
                        serve_connection(TokioIo::new(s), state).await;
                    }
                } else {
                    serve_connection(TokioIo::new(stream), state).await;
                }
            });
        }
    }

    async fn route(&self, req: Request<Incoming>) -> Response<Body> {
        if req.method() != Method::GET {
            return status_json(
                StatusCode::METHOD_NOT_ALLOWED,
                &Message::new("method not allowed"),
            );
        }
        let path = req.uri().path().to_owned();
        match path.as_str() {
            "/health" => status_json(StatusCode::OK, &Message::new("ok")),
            "/ready" => self.ready().await,
            "/metrics" => {
                // Refresh queue/saga gauges so Prometheus scrapes see current depth.
                let _ = self.observer.queues().await;
                let _ = self.observer.sagas().await;
                text(
                    StatusCode::OK,
                    "text/plain; version=0.0.4",
                    self.metrics.render(),
                )
            }
            "/introspect/cluster" => json(StatusCode::OK, &self.observer.cluster().await),
            "/introspect/raft-groups" => json(StatusCode::OK, &self.observer.raft_groups().await),
            "/introspect/actors" => json(StatusCode::OK, &self.observer.actors().await),
            "/introspect/queues" => json(StatusCode::OK, &self.observer.queues().await),
            "/introspect/sagas" => json(StatusCode::OK, &self.observer.sagas().await),
            "/dashboard" => text(
                StatusCode::OK,
                "text/html; charset=utf-8",
                DASHBOARD_HTML.to_owned(),
            ),
            "/dashboard/events" => self.sse(),
            other => self.dynamic(other).await,
        }
    }

    async fn dynamic(&self, path: &str) -> Response<Body> {
        if let Some(id) = path.strip_prefix("/introspect/actors/") {
            return match self.observer.actor(id).await {
                Some(actor) => json(StatusCode::OK, &actor),
                None => status_json(StatusCode::NOT_FOUND, &Message::new("no such actor")),
            };
        }
        if let Some(raw) = path.strip_prefix("/introspect/node/") {
            return match raw.parse::<u64>() {
                Ok(id) => match self.observer.node(id).await {
                    Some(node) => json(StatusCode::OK, &node),
                    None => status_json(StatusCode::NOT_FOUND, &Message::new("no such node")),
                },
                Err(_) => status_json(StatusCode::BAD_REQUEST, &Message::new("invalid node id")),
            };
        }
        status_json(StatusCode::NOT_FOUND, &Message::new("not found"))
    }

    async fn ready(&self) -> Response<Body> {
        let readiness = self.observer.readiness().await;
        let code = if readiness.is_ready() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        json(code, &readiness)
    }

    fn sse(&self) -> Response<Body> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(64);
        let mut sub = self.events.subscribe();
        tokio::spawn(async move {
            // Open the stream immediately so proxies flush headers.
            if tx
                .send(Ok(Frame::data(Bytes::from_static(b": connected\n\n"))))
                .await
                .is_err()
            {
                return;
            }
            while let Some(event) = sub.recv().await {
                let json = serde_json::to_string(&event).unwrap_or_default();
                let msg = format!("data: {json}\n\n");
                if tx.send(Ok(Frame::data(Bytes::from(msg)))).await.is_err() {
                    break; // client disconnected
                }
            }
        });
        let body = StreamBody::new(ReceiverStream::new(rx)).boxed();
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .body(body)
            .expect("valid response")
    }
}

async fn serve_connection<S>(io: TokioIo<S>, state: Arc<AdminServer>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |req| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(state.route(req).await) }
    });
    let _ = http1::Builder::new().serve_connection(io, service).await;
}

/// A trivial JSON message envelope for status endpoints.
#[derive(serde::Serialize)]
struct Message<'a> {
    status: &'a str,
}

impl<'a> Message<'a> {
    fn new(status: &'a str) -> Self {
        Self { status }
    }
}

fn full(bytes: Bytes) -> Body {
    Full::new(bytes).boxed()
}

fn json<T: serde::Serialize>(status: StatusCode, value: &T) -> Response<Body> {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full(Bytes::from(body)))
        .expect("valid response")
}

fn status_json(status: StatusCode, value: &Message<'_>) -> Response<Body> {
    json(status, value)
}

fn text(status: StatusCode, content_type: &str, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", content_type.to_owned())
        .body(full(Bytes::from(body)))
        .expect("valid response")
}
