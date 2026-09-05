use std::sync::Arc;

use super::super::app::TrembitaApp;
use super::config::{GatewayConfig, GatewayConfigError};
use super::drain::{self, ConnectionTracker, GatewayHandle};
use super::router::build_gateway_service_with_tracker;

/// Spawn the gateway HTTP server; returns a [`GatewayHandle`] for graceful drain.
///
/// # Errors
/// Returns [`GatewayConfigError`] when product APIs require identity, or
/// [`std::io::Error`] when the listen socket cannot be bound or TLS PEM material is invalid.
pub async fn spawn_gateway(
    app: Arc<TrembitaApp>,
    config: GatewayConfig,
) -> Result<GatewayHandle, GatewaySpawnError> {
    let addr = config.addr;
    let drain_timeout = config.drain_timeout;
    let tls_paths = config.tls.clone();
    let workload = app.cluster().workload_runtime();
    let connections = workload.as_ref().map_or_else(
        || Arc::new(ConnectionTracker::default()),
        |w| w.connections(),
    );
    let mut service =
        build_gateway_service_with_tracker(&app, config, Some(Arc::clone(&connections)))?;
    if let Some(wl) = &workload {
        service.compute_pool = Some(wl.pool());
    }
    let tls = tls_paths
        .as_ref()
        .map(trembita_dashboard::admin_tls_config)
        .transpose()
        .map_err(|e| {
            GatewaySpawnError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
        })?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(GatewaySpawnError::Io)?;
    let scheme = if tls.is_some() { "https" } else { "http" };
    eprintln!("trembita: gateway listening on {scheme}://{addr}");
    Ok(drain::spawn_serve(
        listener,
        service,
        connections,
        drain_timeout,
        tls,
    ))
}

/// Gateway spawn failures (config validation or I/O).
#[derive(Debug, thiserror::Error)]
pub enum GatewaySpawnError {
    /// Invalid gateway wiring.
    #[error(transparent)]
    Config(#[from] GatewayConfigError),
    /// Invalid gateway surface wiring.
    #[error(transparent)]
    Build(#[from] trembita_http::GatewayBuildError),
    /// Listen/bind or TLS load failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
