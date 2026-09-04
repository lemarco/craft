use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;

use super::super::app::TrembitaApp;
use super::GatewayTlsPaths;
use super::config::{DEFAULT_GATEWAY_DRAIN_TIMEOUT, GatewayConfig, GatewayRoutesFn};
use super::identity::{self, GatewayIdentity, SessionKey};
use super::state::TrembitaGatewayState;

/// Product HTTP gateway: listen address, custom Axum routes, optional built-in APIs.
#[allow(clippy::struct_excessive_bools)] // feature toggles map 1:1 to optional product APIs.
pub struct GatewayOpts {
    addr: SocketAddr,
    jobs_api: bool,
    actors_api: bool,
    workflows_api: bool,
    introspect_api: bool,
    identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    routes: Option<GatewayRoutesFn>,
    drain_timeout: Duration,
    tls: Option<GatewayTlsPaths>,
    protect_apis: bool,
    rate_limit_per_sec: Option<u32>,
}

impl fmt::Debug for GatewayOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayOpts")
            .field("addr", &self.addr)
            .field("jobs_api", &self.jobs_api)
            .field("actors_api", &self.actors_api)
            .field("workflows_api", &self.workflows_api)
            .field("introspect_api", &self.introspect_api)
            .field("identity", &self.identity.as_ref().map(|_| "<extractor>"))
            .field("routes", &self.routes.as_ref().map(|_| "<router>"))
            .field("drain_timeout", &self.drain_timeout)
            .field("tls", &self.tls.as_ref().map(|_| "<pem>"))
            .field("protect_apis", &self.protect_apis)
            .field("rate_limit_per_sec", &self.rate_limit_per_sec)
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
            introspect_api: false,
            identity: None,
            routes: None,
            drain_timeout: DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            tls: None,
            protect_apis: false,
            rate_limit_per_sec: None,
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

    /// Enable or disable read-only `/introspect/*` routes ([`IntrospectApi`](trembita_http::IntrospectApi)).
    #[must_use]
    pub fn with_introspect_api(mut self, enabled: bool) -> Self {
        self.introspect_api = enabled;
        self
    }

    /// Require [`Self::identity`] on built-in `/jobs/*`, `/actors/*`, `/workflows/*`, and `/introspect/*` routes.
    ///
    /// Custom routes from [`.routes`](Self::routes) are unchanged — attach auth there explicitly.
    #[must_use]
    pub fn protect_product_apis(mut self, enabled: bool) -> Self {
        self.protect_apis = enabled;
        self
    }

    /// Cap gateway-wide HTTP throughput at `limit` requests per second (`429` when exceeded).
    #[must_use]
    pub fn rate_limit_per_sec(mut self, limit: u32) -> Self {
        self.rate_limit_per_sec = Some(limit.max(1));
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

    /// Collect gateway wiring for [`super::build_gateway_router`] / [`super::spawn_gateway`].
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
            introspect_api: self.introspect_api,
            identity: self.identity,
            routes: self.routes,
            drain_timeout: self.drain_timeout,
            tls: self.tls,
            protect_apis: self.protect_apis,
            rate_limit_per_sec: self.rate_limit_per_sec,
        }
    }
}
