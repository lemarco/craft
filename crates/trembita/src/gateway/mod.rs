//! Product HTTP / WebSocket gateway for [`TrembitaApp`](super::app::TrembitaApp).
//!
//! Several hostnames on one listen port: use [`HostRouter`] (re-exported from
//! `trembita-http`) in [`GatewayOpts::routes`] — strict by default, opt-in
//! [`HostRouter::local_dev_fallback`] for loopback only.

mod compute;
mod drain;
mod identity;
mod session;

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderMap, Method, Uri};
use trembita_dashboard::AdminTlsPaths;

use super::app::TrembitaApp;

/// PEM paths for server-only TLS on the product gateway (HTTPS / WSS).
pub type GatewayTlsPaths = AdminTlsPaths;

pub use drain::{ConnectionGuard, ConnectionTracker, GatewayHandle};
pub use identity::{
    ExtractedIdentity, GatewayBearerIdentity, GatewayIdentity, GatewayRequest,
    GatewayTokenIdentity, IdentityError, IdentityTypeError, SessionKey,
};
pub use session::{NoWorkerError, OpenActorSessionError, SessionHandle};

/// Default gateway drain when [`GatewayOpts::drain_timeout`] is omitted.
pub const DEFAULT_GATEWAY_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Default wait for job queue consumers to finish in-flight work during shutdown.
pub const DEFAULT_CONSUMER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Product HTTP gateway: listen address, custom Axum routes, optional built-in APIs.
#[allow(clippy::struct_excessive_bools)] // feature toggles map 1:1 to optional product APIs.
pub struct GatewayOpts {
    addr: SocketAddr,
    jobs_api: bool,
    actors_api: bool,
    workflows_api: bool,
    identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    routes: Option<GatewayRoutesFn>,
    drain_timeout: Duration,
    tls: Option<GatewayTlsPaths>,
    protect_apis: bool,
}

impl fmt::Debug for GatewayOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayOpts")
            .field("addr", &self.addr)
            .field("jobs_api", &self.jobs_api)
            .field("actors_api", &self.actors_api)
            .field("workflows_api", &self.workflows_api)
            .field("identity", &self.identity.as_ref().map(|_| "<extractor>"))
            .field("routes", &self.routes.as_ref().map(|_| "<router>"))
            .field("drain_timeout", &self.drain_timeout)
            .field("tls", &self.tls.as_ref().map(|_| "<pem>"))
            .field("protect_apis", &self.protect_apis)
            .finish()
    }
}

impl GatewayOpts {
    /// Bind address with no routes and no built-in APIs mounted.
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            jobs_api: false,
            actors_api: false,
            workflows_api: false,
            identity: None,
            routes: None,
            drain_timeout: DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            tls: None,
            protect_apis: false,
        }
    }

    /// Public HTTP bind address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// User identity extractor; session key defaults to [`SessionKey`] on `Identity`.
    #[must_use]
    pub fn identity<I>(mut self, extractor: I) -> Self
    where
        I: GatewayIdentity,
        I::Identity: SessionKey,
    {
        self.identity = Some(identity::erase_identity(extractor));
        self
    }

    /// Identity extractor with a custom session-key mapping (when identity ≠ session key).
    #[must_use]
    pub fn identity_mapped<I, F>(mut self, extractor: I, session_key: F) -> Self
    where
        I: GatewayIdentity,
        I::Identity: 'static,
        F: Fn(&I::Identity) -> String + Send + Sync + 'static,
    {
        self.identity = Some(identity::erase_identity_mapped(extractor, session_key));
        self
    }

    /// Max wait for active gateway connections during graceful shutdown.
    #[must_use]
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// Serve the gateway over **TLS** (server-only) using PEM `cert` and `key`.
    ///
    /// WebSocket upgrades on this listener use **WSS** automatically.
    #[must_use]
    pub fn tls(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.tls = Some(GatewayTlsPaths {
            cert: cert.into(),
            key: key.into(),
        });
        self
    }

    /// Enable or disable job queue `/jobs/*` routes.
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

    /// Require [`Self::identity`] on built-in `/jobs/*`, `/actors/*`, and `/workflows/*` routes.
    ///
    /// Custom routes from [`.routes`](Self::routes) are unchanged — attach auth there explicitly.
    #[must_use]
    pub fn protect_product_apis(mut self, enabled: bool) -> Self {
        self.protect_apis = enabled;
        self
    }

    /// Custom Axum routes (WebSocket, authenticated HTTP, …).
    /// Custom Axum routes merged after built-in APIs. Return a [`Router`] from
    /// [`HostRouter::build`](trembita_http::HostRouter::build) when dispatching
    /// several hostnames on one listen port.
    #[must_use]
    pub fn routes<F>(mut self, routes: F) -> Self
    where
        F: FnOnce(TrembitaGatewayState) -> Router + Send + 'static,
    {
        self.routes = Some(Box::new(routes));
        self
    }

    /// Convenience: custom routes with only [`TrembitaApp`] (no identity on state).
    #[must_use]
    pub fn routes_with_app<F>(mut self, routes: F) -> Self
    where
        F: FnOnce(Arc<TrembitaApp>) -> Router + Send + 'static,
    {
        self.routes = Some(Box::new(move |state| routes(state.app)));
        self
    }

    /// Collect gateway wiring for [`build_gateway_router`] / [`spawn_gateway`].
    #[must_use]
    pub fn build_config(self) -> GatewayConfig {
        self.into_config()
    }

    #[must_use]
    pub(crate) fn into_config(self) -> GatewayConfig {
        GatewayConfig {
            addr: self.addr,
            jobs_api: self.jobs_api,
            actors_api: self.actors_api,
            workflows_api: self.workflows_api,
            identity: self.identity,
            routes: self.routes,
            drain_timeout: self.drain_timeout,
            tls: self.tls,
            protect_apis: self.protect_apis,
        }
    }
}

/// Shared Axum state for gateway handlers that need the running app.
#[derive(Clone)]
pub struct TrembitaGatewayState {
    /// Running product app handle.
    pub app: Arc<TrembitaApp>,
    identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    connections: Option<Arc<ConnectionTracker>>,
}

impl fmt::Debug for TrembitaGatewayState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrembitaGatewayState")
            .field("identity", &self.identity.as_ref().map(|_| "<extractor>"))
            .field(
                "connections",
                &self.connections.as_ref().map_or(0, |c| c.active()),
            )
            .finish_non_exhaustive()
    }
}

impl TrembitaGatewayState {
    /// Gateway state with only the app handle (no identity extractor).
    #[must_use]
    pub fn new(app: Arc<TrembitaApp>) -> Self {
        Self {
            app,
            identity: None,
            connections: None,
        }
    }

    /// Track a long-lived connection until the guard drops (WebSocket, SSE, …).
    ///
    /// Short HTTP handlers are tracked automatically by gateway middleware when the
    /// router is built via [`build_gateway_router`] or [`spawn_gateway`].
    #[must_use]
    pub fn track_connection(&self) -> Option<ConnectionGuard<'_>> {
        self.connections.as_ref().map(|c| c.track())
    }

    /// Extract authenticated identity and session key.
    ///
    /// # Errors
    /// Returns [`IdentityError::NotConfigured`] when no extractor was set, or the extractor's error.
    pub async fn extract_session(
        &self,
        req: &GatewayRequest<'_>,
    ) -> Result<ExtractedIdentity, IdentityError> {
        match &self.identity {
            Some(extractor) => extractor.extract_dyn(req).await,
            None => Err(IdentityError::NotConfigured),
        }
    }

    /// [`extract_session`](Self::extract_session) from any HTTP request.
    ///
    /// # Errors
    /// Same as [`Self::extract_session`].
    pub async fn extract_session_from<B>(
        &self,
        req: &axum::http::Request<B>,
    ) -> Result<ExtractedIdentity, IdentityError> {
        let gw_req = GatewayRequest::from_http(req);
        self.extract_session(&gw_req).await
    }

    /// Auth + sticky [`SessionHandle`] in one call.
    ///
    /// # Errors
    /// Returns identity errors, or [`OpenActorSessionError::NoWorker`] when no worker is available.
    pub async fn open_actor_session(
        &self,
        group: &str,
        req: &GatewayRequest<'_>,
        ttl: Option<Duration>,
    ) -> Result<SessionHandle, OpenActorSessionError> {
        let extracted = self.extract_session(req).await?;
        SessionHandle::open_from_extracted(&self.app, group, &extracted, ttl)
            .ok_or_else(|| NoWorkerError(group.to_string()).into())
    }

    /// Like [`Self::open_actor_session`] from any HTTP request.
    ///
    /// # Errors
    /// Same as [`Self::open_actor_session`].
    pub async fn open_actor_session_from<B>(
        &self,
        group: &str,
        req: &axum::http::Request<B>,
        ttl: Option<Duration>,
    ) -> Result<SessionHandle, OpenActorSessionError> {
        let gw_req = GatewayRequest::from_http(req);
        self.open_actor_session(group, &gw_req, ttl).await
    }

    /// [`extract_session`](Self::extract_session) from axum **parts** (WebSocket upgrade handlers).
    ///
    /// Use with [`WebSocketUpgrade`](axum::extract::ws::WebSocketUpgrade) — do not also extract
    /// [`Request`](axum::http::Request); the upgrade consumes the body.
    ///
    /// # Errors
    /// Same as [`Self::extract_session`].
    pub async fn extract_session_parts(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Result<ExtractedIdentity, IdentityError> {
        let gw_req = GatewayRequest::from_parts(method, uri, headers);
        self.extract_session(&gw_req).await
    }

    /// Like [`Self::open_actor_session`] from axum **parts** (WebSocket upgrade handlers).
    ///
    /// # Errors
    /// Same as [`Self::open_actor_session`].
    pub async fn open_actor_session_parts(
        &self,
        group: &str,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        ttl: Option<Duration>,
    ) -> Result<SessionHandle, OpenActorSessionError> {
        let gw_req = GatewayRequest::from_parts(method, uri, headers);
        self.open_actor_session(group, &gw_req, ttl).await
    }

    /// Gateway state with app handle and identity extractor.
    #[must_use]
    pub fn with_identity<I>(app: Arc<TrembitaApp>, extractor: I) -> Self
    where
        I: GatewayIdentity,
        I::Identity: SessionKey,
    {
        Self {
            app,
            identity: Some(identity::erase_identity(extractor)),
            connections: None,
        }
    }

    /// Like [`Self::with_identity`] but map session key separately from identity.
    #[must_use]
    pub fn with_identity_mapped<I, F>(app: Arc<TrembitaApp>, extractor: I, session_key: F) -> Self
    where
        I: GatewayIdentity,
        I::Identity: 'static,
        F: Fn(&I::Identity) -> String + Send + Sync + 'static,
    {
        Self {
            app,
            identity: Some(identity::erase_identity_mapped(extractor, session_key)),
            connections: None,
        }
    }

    pub(crate) fn from_parts(
        app: Arc<TrembitaApp>,
        identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
        connections: Option<Arc<ConnectionTracker>>,
    ) -> Self {
        Self {
            app,
            identity,
            connections,
        }
    }
}

/// User-supplied gateway router builder (captures [`TrembitaGatewayState`] in handlers).
pub type GatewayRoutesFn = Box<dyn FnOnce(TrembitaGatewayState) -> Router + Send>;

/// Gateway listen address and route wiring collected on [`super::app::TrembitaAppBuilder`].
#[allow(clippy::struct_excessive_bools)] // feature toggles map 1:1 to optional product APIs.
pub struct GatewayConfig {
    /// Public HTTP bind address.
    pub addr: SocketAddr,
    /// Mount job queue `/jobs/*` routes (requires `http-jobs` feature).
    pub jobs_api: bool,
    /// Mount `/actors/*` cast + ask routes (requires `http-jobs` feature).
    pub actors_api: bool,
    /// Optional custom routes (WebSocket, sync HTTP, etc.).
    pub routes: Option<GatewayRoutesFn>,
    /// Mount `/workflows/*` routes when a plan builder is configured.
    pub workflows_api: bool,
    /// Optional identity extractor ([`GatewayOpts::identity`]).
    pub(crate) identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    /// Graceful drain timeout for active connections.
    pub drain_timeout: Duration,
    /// Optional server-only TLS (`TREMBITA_GATEWAY_TLS_*` / [`GatewayOpts::tls`]).
    pub tls: Option<GatewayTlsPaths>,
    /// When `true`, built-in product APIs require [`GatewayOpts::identity`].
    pub protect_apis: bool,
}

/// Build the gateway router: custom routes first, then optional product APIs.
///
/// Installs connection-tracking middleware on the merged router so HTTP handlers
/// do not need to call [`TrembitaGatewayState::track_connection`] manually.
pub fn build_gateway_router(app: Arc<TrembitaApp>, config: GatewayConfig) -> Router {
    let connections = Arc::new(ConnectionTracker::default());
    build_gateway_router_with_tracker(app, config, Some(connections))
}

fn build_gateway_router_with_tracker(
    app: Arc<TrembitaApp>,
    config: GatewayConfig,
    connections: Option<Arc<ConnectionTracker>>,
) -> Router {
    let GatewayConfig {
        addr: _,
        jobs_api,
        actors_api,
        workflows_api,
        identity,
        routes,
        drain_timeout: _,
        tls: _,
        protect_apis,
    } = config;

    let auth = if protect_apis {
        identity.clone().map(identity_auth_fn)
    } else {
        None
    };

    let state = TrembitaGatewayState::from_parts(Arc::clone(&app), identity, connections.clone());
    let mut router = routes.map_or_else(Router::new, |f| f(state));

    #[cfg(feature = "http-jobs")]
    {
        if workflows_api {
            let api = TrembitaApp::workflows_api(Arc::clone(&app));
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }

        if actors_api {
            let api = TrembitaApp::actors_api(Arc::clone(&app));
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }

        if jobs_api {
            let api = TrembitaApp::jobs_api(app);
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }
    }
    #[cfg(not(feature = "http-jobs"))]
    {
        let _ = (jobs_api, actors_api, workflows_api, app);
    }

    if let Some(connections) = connections {
        router = router.layer(axum::middleware::from_fn_with_state(
            connections,
            drain::track_connection,
        ));
    }

    router
}

/// Spawn the gateway HTTP server; returns a [`GatewayHandle`] for graceful drain.
///
/// When a [`crate::WorkloadRuntime`] is configured on the cluster, reuses its
/// [`ConnectionTracker`] for drain, ingress counting, and compute-token middleware.
///
/// # Errors
/// Returns [`std::io::Error`] when the listen socket cannot be bound or TLS PEM
/// material is invalid.
pub async fn spawn_gateway(
    app: Arc<TrembitaApp>,
    config: GatewayConfig,
) -> std::io::Result<GatewayHandle> {
    let addr = config.addr;
    let drain_timeout = config.drain_timeout;
    let tls_paths = config.tls.clone();
    let workload = app.cluster().workload_runtime();
    let connections = workload.as_ref().map_or_else(
        || Arc::new(ConnectionTracker::default()),
        |w| w.connections(),
    );
    let mut router =
        build_gateway_router_with_tracker(Arc::clone(&app), config, Some(Arc::clone(&connections)));
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
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

#[cfg(feature = "http-jobs")]
fn identity_auth_fn(extractor: Arc<dyn identity::DynGatewayIdentity>) -> trembita_http::AuthFn {
    Arc::new(move |method, uri, headers| {
        let extractor = Arc::clone(&extractor);
        Box::pin(async move {
            let req = GatewayRequest::from_parts(&method, &uri, &headers);
            extractor
                .extract_dyn(&req)
                .await
                .map_err(|e| trembita_http::JobsApiError::Unauthorized(e.to_string()))?;
            Ok(())
        })
    })
}
