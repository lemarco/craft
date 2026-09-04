use std::sync::Arc;

use super::super::app::TrembitaApp;
use super::compute;
use super::config::{GatewayConfig, GatewayConfigError};
use super::drain::{self, ConnectionTracker, GatewayHandle};
use super::router::build_gateway_router_with_tracker;

/// Spawn the gateway HTTP server; returns a [`GatewayHandle`] for graceful drain.
///
/// When a [`crate::WorkloadRuntime`] is configured on the cluster, reuses its
/// [`ConnectionTracker`] for drain, ingress counting, and compute-token middleware.
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
    let mut router =
        build_gateway_router_with_tracker(&app, config, Some(Arc::clone(&connections)))?;
    if let Some(wl) = &workload {
        router = router.layer(axum::middleware::from_fn_with_state(
            wl.pool(),
            compute::acquire_compute_token,
        ));
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
        router,
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
    /// Listen/bind or TLS load failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
