//! Product HTTP / WebSocket gateway for [`CraftyApp`](super::app::CraftyApp).
//!
//! Mount custom Axum routes and (with the `http-jobs` feature) tier C job paths on a
//! separate listener from the admin dashboard and the mTLS crafty wire.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;

use super::app::CraftyApp;

/// Shared Axum state for gateway handlers that need the running app.
#[derive(Clone)]
pub struct CraftyGatewayState {
    /// Running product app handle.
    pub app: Arc<CraftyApp>,
}

/// User-supplied gateway router builder (captures [`Arc<CraftyApp>`] in handlers).
pub type GatewayRoutesFn = Box<dyn FnOnce(Arc<CraftyApp>) -> Router + Send>;

/// Gateway listen address and route wiring collected on [`super::app::CraftyAppBuilder`].
pub struct GatewayConfig {
    /// Public HTTP bind address.
    pub addr: SocketAddr,
    /// Mount tier C `/jobs/*` routes (requires `http-jobs` feature).
    pub jobs_api: bool,
    /// Mount `/actors/*` cast + ask routes (requires `http-jobs` feature).
    pub actors_api: bool,
    /// Optional custom routes (WebSocket, sync HTTP, etc.).
    pub routes: Option<GatewayRoutesFn>,
}

/// Build the gateway router: custom routes first, then optional product APIs.
pub fn build_gateway_router(app: Arc<CraftyApp>, config: GatewayConfig) -> Router {
    let GatewayConfig {
        addr: _,
        jobs_api,
        actors_api,
        routes,
    } = config;

    let mut router = routes.map_or_else(Router::new, |f| f(Arc::clone(&app)));

    if actors_api {
        let api = CraftyApp::actors_api(Arc::clone(&app));
        router = router.merge(api.router().with_state(Arc::new(api.into_state())));
    }

    if jobs_api {
        let api = CraftyApp::jobs_api(app);
        router = router.merge(api.router().with_state(Arc::new(api.into_state())));
    }

    router
}

/// Spawn the gateway HTTP server on a background task.
///
/// Binds synchronously before returning so callers can fail fast when the port is
/// taken. Serve errors are logged to stderr; the cluster keeps running.
pub async fn spawn_gateway(app: Arc<CraftyApp>, config: GatewayConfig) -> std::io::Result<()> {
    let addr = config.addr;
    let router = build_gateway_router(app, config);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("crafty: gateway listening on http://{addr}");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("crafty: gateway server on {addr} failed: {e}");
        }
    });
    Ok(())
}

/// Async helper used by [`super::app::CraftyAppBuilder::run_local_until_shutdown`].
pub async fn run_gateway_until_shutdown(
    app: Arc<CraftyApp>,
    config: GatewayConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = config.addr;
    let router = build_gateway_router(app, config);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let serve = axum::serve(listener, router);
    tokio::select! {
        res = serve => {
            res?;
        }
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}
