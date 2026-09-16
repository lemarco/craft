//! Gateway graceful shutdown — stop accept and drain active connections.

use std::sync::Arc;
use std::time::Duration;

pub use trembita_assembly::{ConnectionGuard, ConnectionTracker};

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use super::router::WrappedGatewayService;

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
    service: WrappedGatewayService,
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
            Some(tls) => serve_tls(listener, service, tls, shutdown_rx).await,
            None => serve_plain(listener, service, shutdown_rx).await,
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
    service: WrappedGatewayService,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accept = listener.accept() => {
                let (stream, _) = accept?;
                let service = service.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let hyper_service = TowerToHyperService::new(service);
                    let _ = http1::Builder::new()
                        .serve_connection(io, hyper_service)
                        .with_upgrades()
                        .await;
                });
            }
        }
    }
    Ok(())
}

async fn serve_tls(
    listener: tokio::net::TcpListener,
    service: WrappedGatewayService,
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
                        let service = service.clone();
                        tokio::spawn(async move {
                            let Ok(tls_stream) = acceptor.accept(stream).await else {
                                return;
                            };
                            let io = TokioIo::new(tls_stream);
                            let hyper_service = TowerToHyperService::new(service);
                            let _ = http1::Builder::new()
                                .serve_connection(io, hyper_service)
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
    use super::ConnectionTracker;

    #[test]
    fn connection_guard_decrements_on_drop() {
        let tracker = ConnectionTracker::default();
        {
            let _guard = tracker.track();
            assert_eq!(tracker.active(), 1);
        }
        assert_eq!(tracker.active(), 0);
    }
}
