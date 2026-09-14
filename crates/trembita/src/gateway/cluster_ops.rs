//! Ops HTTP (`/health`, `/metrics`, `/dashboard`, `/introspect/*`) for [`TrembitaCluster`](crate::cluster::TrembitaCluster) without [`TrembitaApp`](crate::TrembitaApp).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use trembita_core::StateMachine;
use trembita_http::{Gateway, GatewayService, RouteTable};

use crate::cluster_handle::TrembitaCluster;

use super::GatewayHandle;
use super::GatewayTlsPaths;
use super::drain::{self, ConnectionTracker};
use super::router::WrappedGatewayService;
use super::spawn::GatewaySpawnError;

/// Ops route table backed by a running cluster (same JSON as [`crate::TrembitaApp::ops_api`]).
#[must_use]
pub fn cluster_ops_route_table<M>(cluster: &TrembitaCluster<M>) -> RouteTable
where
    M: StateMachine + Send + Sync + 'static,
{
    trembita_http::OpsApi::new(
        cluster.introspect_observer(),
        cluster.metrics().clone(),
        cluster.events().clone(),
    )
    .route_table()
}

/// Bind `addr` and serve [`cluster_ops_route_table`] until [`GatewayHandle::drain`].
///
/// # Errors
/// Returns [`GatewaySpawnError`] when route wiring fails or the listen socket cannot be bound.
pub async fn spawn_cluster_ops_http<M>(
    cluster: &TrembitaCluster<M>,
    addr: SocketAddr,
    drain_timeout: Duration,
    tls: Option<GatewayTlsPaths>,
) -> Result<GatewayHandle, GatewaySpawnError>
where
    M: StateMachine + Send + Sync + 'static,
{
    let connections = Arc::new(ConnectionTracker::default());
    let inner: GatewayService = Gateway::new(false)
        .dev_fallback(cluster_ops_route_table(cluster))
        .build_service()
        .map_err(GatewaySpawnError::Build)?;
    let service =
        WrappedGatewayService::from_gateway_service(inner, Some(Arc::clone(&connections)));
    let tls = tls
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
    eprintln!("trembita: ops http listening on {scheme}://{addr}");
    Ok(drain::spawn_serve(
        listener,
        service,
        connections,
        drain_timeout,
        tls,
    ))
}
