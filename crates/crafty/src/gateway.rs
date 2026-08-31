//! Product HTTP / WebSocket gateway for [`CraftyApp`](super::app::CraftyApp).
//!
//! Mount custom Axum routes and (with the `http-jobs` feature) tier C job paths on a
//! separate listener from the admin dashboard and the mTLS crafty wire.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;

use super::app::CraftyApp;

/// Product HTTP gateway: custom Axum routes + optional built-in APIs.
///
/// [`Default`] mounts nothing — set [`.routes`](Self::routes) for your handlers and opt in
/// to `jobs_api` / `actors_api` / `workflows_api` only for local dev behind auth.
#[derive(Default)]
pub struct GatewayOpts {
    /// Mount tier C `/jobs/*` routes (unauthenticated — opt in deliberately).
    pub jobs_api: bool,
    /// Mount `/actors/*` cast + ask routes (unauthenticated — opt in deliberately).
    pub actors_api: bool,
    /// Mount `/workflows/run` and `/workflows/resume` (unauthenticated — opt in deliberately).
    pub workflows_api: bool,
    routes: Option<GatewayRoutesFn>,
}

impl fmt::Debug for GatewayOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayOpts")
            .field("jobs_api", &self.jobs_api)
            .field("actors_api", &self.actors_api)
            .field("workflows_api", &self.workflows_api)
            .field("routes", &self.routes.as_ref().map(|_| "<router>"))
            .finish()
    }
}

impl GatewayOpts {
    /// Enable or disable tier C `/jobs/*` routes.
    #[must_use]
    pub fn with_jobs_api(mut self, enabled: bool) -> Self {
        self.jobs_api = enabled;
        self
    }

    /// Enable or disable `/actors/*` cast + ask routes.
    #[must_use]
    pub fn with_actors_api(mut self, enabled: bool) -> Self {
        self.actors_api = enabled;
        self
    }

    /// Enable or disable `/workflows/run` and `/workflows/resume`.
    #[must_use]
    pub fn with_workflows_api(mut self, enabled: bool) -> Self {
        self.workflows_api = enabled;
        self
    }

    /// Custom Axum routes (WebSocket, authenticated HTTP, …).
    ///
    /// The closure receives [`Arc<CraftyApp>`] — use [`CraftyGatewayState`] or bind `app` directly.
    #[must_use]
    pub fn routes<F>(mut self, routes: F) -> Self
    where
        F: FnOnce(Arc<CraftyApp>) -> Router + Send + 'static,
    {
        self.routes = Some(Box::new(routes));
        self
    }

    pub(crate) fn into_config(self, addr: SocketAddr) -> GatewayConfig {
        GatewayConfig {
            addr,
            jobs_api: self.jobs_api,
            actors_api: self.actors_api,
            workflows_api: self.workflows_api,
            routes: self.routes,
        }
    }
}

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
    /// Mount `/workflows/*` routes when a plan builder is configured.
    pub workflows_api: bool,
}

/// Build the gateway router: custom routes first, then optional product APIs.
pub fn build_gateway_router(app: Arc<CraftyApp>, config: GatewayConfig) -> Router {
    let GatewayConfig {
        addr: _,
        jobs_api,
        actors_api,
        workflows_api,
        routes,
    } = config;

    let mut router = routes.map_or_else(Router::new, |f| f(app.clone()));

    if workflows_api {
        let api = CraftyApp::workflows_api(Arc::clone(&app));
        router = router.merge(api.router().with_state(Arc::new(api.into_state())));
    }

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
///
/// # Errors
/// Returns [`std::io::Error`] when the listen socket cannot be bound.
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
