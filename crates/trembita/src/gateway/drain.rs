//! Gateway graceful shutdown — stop accept and drain active connections.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

/// Tracks live gateway connections (WebSocket, long-poll, …).
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    active: AtomicUsize,
}

impl ConnectionTracker {
    /// Increment active connection count; decrements when the guard drops.
    ///
    /// Short HTTP handlers are tracked automatically by gateway middleware;
    /// call this for long-lived work (WebSocket, SSE, …).
    #[must_use]
    pub fn track(&self) -> ConnectionGuard<'_> {
        self.active.fetch_add(1, Ordering::SeqCst);
        ConnectionGuard { tracker: self }
    }

    /// Number of connections still open.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

/// Axum middleware: hold a connection slot for the duration of each HTTP request.
pub async fn track_connection(
    State(connections): State<Arc<ConnectionTracker>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let _guard = connections.track();
    next.run(request).await
}

/// RAII guard — decrements [`ConnectionTracker`] on drop.
pub struct ConnectionGuard<'a> {
    tracker: &'a ConnectionTracker,
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.tracker.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Handle returned by [`super::spawn_gateway`] for graceful drain.
pub struct GatewayHandle {
    shutdown_tx: watch::Sender<bool>,
    serve: JoinHandle<()>,
    connections: Arc<ConnectionTracker>,
    drain_timeout: Duration,
}

impl GatewayHandle {
    /// Shared connection tracker (wire into [`super::TrembitaGatewayState`]).
    #[must_use]
    pub fn connections(&self) -> Arc<ConnectionTracker> {
        Arc::clone(&self.connections)
    }

    /// Stop accepting new connections and wait for in-flight ones (up to timeout).
    pub async fn drain(self) {
        let _ = self.shutdown_tx.send(true);
        let deadline = tokio::time::Instant::now() + self.drain_timeout;
        loop {
            if self.connections.active() == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                eprintln!(
                    "trembita: gateway drain timeout ({:?}) with {} connection(s) still active",
                    self.drain_timeout,
                    self.connections.active()
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = self.serve.await;
    }
}

pub(crate) fn spawn_serve(
    listener: tokio::net::TcpListener,
    router: Router,
    connections: Arc<ConnectionTracker>,
    drain_timeout: Duration,
    tls: Option<Arc<ServerConfig>>,
) -> GatewayHandle {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let serve = tokio::spawn(async move {
        let addr = listener
            .local_addr()
            .map_or_else(|_| "?".into(), |a| a.to_string());
        let result = match tls {
            Some(tls) => serve_tls(listener, router, tls, shutdown_rx).await,
            None => serve_plain(listener, router, shutdown_rx).await,
        };
        if let Err(e) = result {
            eprintln!("trembita: gateway server on {addr} failed: {e}");
        }
    });
    GatewayHandle {
        shutdown_tx,
        serve,
        connections,
        drain_timeout,
    }
}

async fn serve_plain(
    listener: tokio::net::TcpListener,
    router: Router,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            while !*shutdown_rx.borrow_and_update() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
}

async fn serve_tls(
    listener: tokio::net::TcpListener,
    router: Router,
    tls: Arc<ServerConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let acceptor = TlsAcceptor::from(tls);
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let acceptor = acceptor.clone();
                        let router = router.clone();
                        tokio::spawn(async move {
                            let Ok(tls_stream) = acceptor.accept(stream).await else {
                                return;
                            };
                            let io = TokioIo::new(tls_stream);
                            let service = TowerToHyperService::new(router);
                            let _ = http1::Builder::new()
                                .serve_connection(io, service)
                                .with_upgrades()
                                .await;
                        });
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::{ConnectionTracker, track_connection};

    #[tokio::test]
    async fn track_connection_middleware_holds_slot_for_request() {
        let connections = Arc::new(ConnectionTracker::default());
        let during = Arc::clone(&connections);
        let router = Router::new()
            .route(
                "/",
                get(move || {
                    let during = Arc::clone(&during);
                    async move {
                        assert_eq!(during.active(), 1);
                        "ok"
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&connections),
                track_connection,
            ));

        let response = router
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(connections.active(), 0);
    }
}
