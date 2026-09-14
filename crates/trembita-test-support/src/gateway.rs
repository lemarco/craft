//! Gateway surface helpers for integration tests.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use trembita::GatewayConfig;
use trembita::TrembitaApp;
use trembita::cluster::build_gateway_service;
use trembita::{Gateway, GatewayOpts, TrembitaGatewayState};
use trembita_http::AuthMode;

/// Bind an ephemeral port and serve `config` until the runtime shuts down.
///
/// # Panics
/// When gateway wiring is invalid or the listen socket cannot be bound.
pub async fn spawn_test_gateway(app: &Arc<TrembitaApp>, config: GatewayConfig) -> SocketAddr {
    let service = build_gateway_service(app, config).expect("gateway service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway test listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
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
    });
    addr
}

/// Dev-fallback gateway with job routes (open auth).
#[must_use]
pub fn gateway_jobs_surfaces(state: TrembitaGatewayState) -> Gateway {
    let app = Arc::clone(&state.app);
    Gateway::new(false).dev_fallback(TrembitaApp::jobs_api(app).route_table())
}

/// Dev-fallback gateway with identity-protected job routes.
#[must_use]
pub fn gateway_jobs_surfaces_identity(state: TrembitaGatewayState) -> Gateway {
    let app = Arc::clone(&state.app);
    Gateway::new(false).dev_fallback(
        TrembitaApp::jobs_api(app)
            .route_table()
            .with_auth_mode(AuthMode::Identity),
    )
}

/// Dev-fallback gateway with workflow routes.
#[must_use]
pub fn gateway_workflows_surfaces(state: TrembitaGatewayState) -> Gateway {
    let app = Arc::clone(&state.app);
    Gateway::new(false).dev_fallback(TrembitaApp::workflows_api(app).route_table())
}

/// Dev-fallback gateway with actor routes.
#[must_use]
pub fn gateway_actors_surfaces(state: TrembitaGatewayState) -> Gateway {
    let app = Arc::clone(&state.app);
    Gateway::new(false).dev_fallback(TrembitaApp::actors_api(app).route_table())
}

/// Dev-fallback gateway with introspection routes.
#[must_use]
pub fn gateway_introspect_surfaces(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(state.app.introspect_api().route_table())
}

/// Dev-fallback gateway with identity-protected introspection routes.
#[must_use]
pub fn gateway_introspect_surfaces_identity(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(
        state
            .app
            .introspect_api()
            .route_table()
            .with_auth_mode(AuthMode::Identity),
    )
}

/// Dev-fallback gateway with ops routes (health, metrics, dashboard, introspect).
#[must_use]
pub fn gateway_ops_surfaces(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(state.app.ops_api().route_table())
}

/// Ops route table for a running `TrembitaCluster` (facade integration tests).
#[must_use]
pub fn cluster_ops_route_table<M>(
    cluster: &trembita::cluster::TrembitaCluster<M>,
) -> trembita_http::RouteTable
where
    M: trembita_core::StateMachine + Send + Sync + 'static,
{
    trembita::cluster::cluster_ops_route_table(cluster)
}

/// Bind an ephemeral port and serve ops routes for `cluster` until shutdown.
///
/// # Panics
/// When gateway wiring is invalid or the listen socket cannot be bound.
pub async fn spawn_cluster_ops_gateway<M>(
    cluster: &trembita::cluster::TrembitaCluster<M>,
) -> SocketAddr
where
    M: trembita_core::StateMachine + Send + Sync + 'static,
{
    let gateway = Gateway::new(false).dev_fallback(cluster_ops_route_table(cluster));
    let service = gateway.build_service().expect("gateway service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway test listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
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
    });
    addr
}

/// Gateway config with job routes on dev fallback.
#[must_use]
pub fn gateway_jobs_config() -> GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .surfaces(gateway_jobs_surfaces)
        .build_config()
}

/// Gateway config with identity-protected job routes.
#[must_use]
pub fn gateway_jobs_config_identity<I>(identity: I) -> GatewayConfig
where
    I: trembita::GatewayIdentity + 'static,
    <I as trembita::GatewayIdentity>::Identity: trembita::SessionKey,
{
    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .identity(identity)
        .surfaces(gateway_jobs_surfaces_identity)
        .build_config()
}

/// Gateway config with workflow routes.
#[must_use]
pub fn gateway_workflows_config() -> GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .surfaces(gateway_workflows_surfaces)
        .build_config()
}

/// Gateway config with introspection routes (identity-protected).
#[must_use]
pub fn gateway_introspect_config_identity<I>(identity: I) -> GatewayConfig
where
    I: trembita::GatewayIdentity + 'static,
    <I as trembita::GatewayIdentity>::Identity: trembita::SessionKey,
{
    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .identity(identity)
        .surfaces(gateway_introspect_surfaces_identity)
        .build_config()
}

/// Gateway config with ops routes (health, metrics, dashboard, introspect).
#[must_use]
pub fn gateway_ops_config() -> GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .surfaces(gateway_ops_surfaces)
        .build_config()
}
