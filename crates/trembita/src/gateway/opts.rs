use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use trembita_http::Gateway;

use super::GatewayTlsPaths;
use super::config::{DEFAULT_GATEWAY_DRAIN_TIMEOUT, GatewayConfig, GatewaySurfacesFn};
use super::identity::{self, GatewayIdentity, SessionKey};
use super::state::TrembitaGatewayState;

/// Product + ops HTTP listener: bind address, custom surfaces, optional TLS.
pub struct GatewayOpts {
    addr: SocketAddr,
    identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    surfaces: Option<GatewaySurfacesFn>,
    drain_timeout: Duration,
    tls: Option<GatewayTlsPaths>,
    rate_limit_per_sec: Option<u32>,
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
    /// Bind address with no routes — wire surfaces via [`.surfaces`](Self::surfaces).
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            identity: None,
            surfaces: None,
            drain_timeout: DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            tls: None,
            rate_limit_per_sec: None,
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

    /// Custom gateway surfaces (product routes, ops routes, WebSocket, multi-host, …).
    #[must_use]
    pub fn surfaces<F>(mut self, surfaces: F) -> Self
    where
        F: FnOnce(TrembitaGatewayState) -> Gateway + Send + 'static,
    {
        self.surfaces = Some(Box::new(surfaces));
        self
    }

    /// Collect gateway wiring for [`super::build_gateway_service`] / [`super::spawn_gateway`].
    #[must_use]
    pub fn build_config(self) -> GatewayConfig {
        self.into_config()
    }

    #[must_use]
    pub(crate) fn into_config(self) -> GatewayConfig {
        GatewayConfig {
            addr: self.addr,
            identity: self.identity,
            surfaces: self.surfaces,
            drain_timeout: self.drain_timeout,
            tls: self.tls,
            rate_limit_per_sec: self.rate_limit_per_sec,
        }
    }
}
