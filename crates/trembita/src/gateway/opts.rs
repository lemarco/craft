use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use trembita_http::{AuthMode, Gateway, RouteTable};

use crate::app::DefaultGatewayApis;
use crate::env_config::{AppConfig, app_config_from_env};

use super::GatewayTlsPaths;
use super::config::{DEFAULT_GATEWAY_DRAIN_TIMEOUT, GatewayConfig, GatewaySurfacesFn};
use super::identity::{self, GatewayBearerIdentity, GatewayIdentity, SessionKey};
use super::state::TrembitaGatewayState;

/// Product + ops HTTP listener: bind address, custom surfaces, optional TLS.
pub struct GatewayOpts {
    addr: SocketAddr,
    identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    surfaces: Option<GatewaySurfacesFn>,
    drain_timeout: Duration,
    tls: Option<GatewayTlsPaths>,
    rate_limit_per_sec: Option<u32>,
    websocket_routes: Option<Arc<dyn Fn(TrembitaGatewayState) -> RouteTable + Send + Sync>>,
    ws_mounts: Vec<super::ws::WsMount>,
}

impl fmt::Debug for GatewayOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayOpts")
            .field("addr", &self.addr)
            .field("identity", &self.identity.as_ref().map(|_| "<extractor>"))
            .field("surfaces", &self.surfaces.as_ref().map(|_| "<gateway>"))
            .field("drain_timeout", &self.drain_timeout)
            .field("tls", &self.tls.as_ref().map(|_| "<pem>"))
            .field("rate_limit_per_sec", &self.rate_limit_per_sec)
            .finish()
    }
}

impl GatewayOpts {
    /// TCP bind, TLS, drain, and identity from a parsed [`AppConfig`].
    ///
    /// # Errors
    /// `cfg.http` is `None` (`TREMBITA_HTTP=-` — no TCP listener).
    pub fn from_config(cfg: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let Some(addr) = cfg.http else {
            return Err(
                "TREMBITA_HTTP=- disables the TCP gateway; omit .gateway() or enable HTTP on TREMBITA_LISTEN"
                    .into(),
            );
        };
        let mut opts = Self::new(addr)
            .drain_timeout(cfg.http_drain_timeout)
            .identity(GatewayBearerIdentity::from_env());
        if let Some((cert, key)) = cfg.http_tls.clone() {
            opts = opts.tls(cert, key);
        }
        Ok(opts)
    }

    /// TCP bind, TLS, drain, and identity from `TREMBITA_LISTEN` / `TREMBITA_HTTP_TLS_*` ([env.md](../../../docs/env.md)).
    ///
    /// # Errors
    /// Invalid env or `TREMBITA_HTTP=-` (no TCP listener).
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_config(&app_config_from_env()?)
    }

    /// Bind address with no routes — wire surfaces via [`.surfaces`](Self::surfaces) or [`.default_surfaces`](Self::default_surfaces).
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            identity: None,
            surfaces: None,
            drain_timeout: DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            tls: None,
            rate_limit_per_sec: None,
            websocket_routes: None,
            ws_mounts: Vec::new(),
        }
    }

    /// Public HTTP bind address (TCP).
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
    #[must_use]
    pub fn tls(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.tls = Some(GatewayTlsPaths {
            cert: cert.into(),
            key: key.into(),
        });
        self
    }

    /// Cap gateway-wide HTTP throughput at `limit` requests per second (`429` when exceeded).
    #[must_use]
    pub fn rate_limit_per_sec(mut self, limit: u32) -> Self {
        self.rate_limit_per_sec = Some(limit.max(1));
        self
    }

    /// Built-in ops + product APIs ([`crate::TrembitaApp::default_surfaces`]).
    #[must_use]
    pub fn default_surfaces(mut self, is_production: bool, apis: DefaultGatewayApis) -> Self {
        self.surfaces = Some(Box::new(move |state| {
            crate::TrembitaApp::default_surfaces(state, is_production, apis)
        }));
        self
    }

    /// Custom gateway surfaces (product routes, ops routes, WebSocket, multi-host, …).
    #[must_use]
    pub fn surfaces<F>(mut self, surfaces: F) -> Self
    where
        F: FnOnce(TrembitaGatewayState) -> Gateway + Send + 'static,
    {
        self.surfaces = Some(Box::new(surfaces));
        self
    }

    /// Merge WebSocket [`RouteTable`] entries on the default product listener (multi-path WS).
    #[must_use]
    pub fn websocket_routes<F>(mut self, routes: F) -> Self
    where
        F: Fn(TrembitaGatewayState) -> RouteTable + Send + Sync + 'static,
    {
        self.websocket_routes = Some(Arc::new(routes));
        self
    }

    /// Declarative mount (broadcast / notify / raw echo) — see [`super::ws::WsMount`].
    #[must_use]
    pub fn ws(mut self, mount: super::ws::WsMount) -> Self {
        self.ws_mounts.push(mount);
        self
    }

    /// Sticky-session WebSocket to actor group `group` at `path` (identity auth).
    #[must_use]
    pub fn realtime_ws<F>(self, path: &str, group: &str, ttl: Duration, on_connected: F) -> Self
    where
        F: Fn(
                super::ws::StickyWs,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.realtime_ws_auth(path, group, ttl, AuthMode::Identity, on_connected)
    }

    /// Sticky WebSocket with explicit auth mode.
    #[must_use]
    pub fn realtime_ws_auth<F>(
        mut self,
        path: &str,
        group: &str,
        ttl: Duration,
        auth: AuthMode,
        on_connected: F,
    ) -> Self
    where
        F: Fn(
                super::ws::StickyWs,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let path = path.to_string();
        let group = group.to_string();
        let on_connected = Arc::new(on_connected);
        let prev = self.websocket_routes.take();
        let mounts = std::mem::take(&mut self.ws_mounts);
        self.websocket_routes = Some(Arc::new(move |state| {
            let mut table = RouteTable::new();
            table = super::ws::apply_ws_mounts(table, state.clone(), mounts.clone());
            table = super::ws::mount_sticky_websocket(
                table,
                &path,
                auth,
                state.clone(),
                &group,
                Some(ttl),
                {
                    let on_connected = Arc::clone(&on_connected);
                    move |sticky| {
                        let on_connected = Arc::clone(&on_connected);
                        Box::pin(async move { on_connected(sticky).await })
                    }
                },
            );
            if let Some(prev) = &prev {
                table = table.merge(prev(state));
            }
            table
        }));
        self
    }

    /// Collect gateway wiring for [`super::build_gateway_service`] / [`super::spawn_gateway`].
    #[must_use]
    pub fn build_config(self) -> GatewayConfig {
        self.into_config()
    }

    #[must_use]
    pub(crate) fn into_config(self) -> GatewayConfig {
        let mut websocket_routes = self.websocket_routes;
        if !self.ws_mounts.is_empty() {
            let mounts = self.ws_mounts;
            let prev = websocket_routes;
            websocket_routes = Some(Arc::new(move |state| {
                let mut table = RouteTable::new();
                table = super::ws::apply_ws_mounts(table, state.clone(), mounts.clone());
                if let Some(prev) = &prev {
                    table = table.merge(prev(state));
                }
                table
            }));
        }
        GatewayConfig {
            addr: self.addr,
            identity: self.identity,
            surfaces: self.surfaces,
            websocket_routes,
            drain_timeout: self.drain_timeout,
            tls: self.tls,
            rate_limit_per_sec: self.rate_limit_per_sec,
        }
    }
}
