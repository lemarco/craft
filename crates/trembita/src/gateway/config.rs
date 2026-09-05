use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use trembita_http::Gateway;

use super::GatewayTlsPaths;
use super::identity;
use super::state::TrembitaGatewayState;

/// Maximum HTTP request body size on the product gateway (matches QUIC wire cap).
pub const GATEWAY_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Invalid gateway wiring collected at router build / spawn time.
#[derive(Debug, thiserror::Error)]
pub enum GatewayConfigError {
    /// Surface wiring failed at build time.
    #[error(transparent)]
    Build(#[from] trembita_http::GatewayBuildError),
}

/// Validate gateway wiring before bind.
pub fn validate_gateway_config(_config: &GatewayConfig) -> Result<(), GatewayConfigError> {
    Ok(())
}

/// Read `GATEWAY_TOKEN` or `TREMBITA_GATEWAY_TOKEN` when non-empty.
#[must_use]
pub fn gateway_token_from_env() -> Option<String> {
    ["GATEWAY_TOKEN", "TREMBITA_GATEWAY_TOKEN"]
        .into_iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
}

/// Default gateway drain when [`super::GatewayOpts::drain_timeout`] is omitted.
pub const DEFAULT_GATEWAY_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Default wait for job queue consumers to finish in-flight work during shutdown.
pub const DEFAULT_CONSUMER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// User-supplied gateway surface builder (captures [`TrembitaGatewayState`] in handlers).
pub type GatewaySurfacesFn = Box<dyn FnOnce(TrembitaGatewayState) -> Gateway + Send>;

/// Gateway listen address and route wiring collected on [`super::super::app::TrembitaAppBuilder`].
pub struct GatewayConfig {
    /// Public HTTP bind address (TCP).
    pub addr: SocketAddr,
    /// Optional custom surfaces (WebSocket, sync HTTP, etc.).
    pub surfaces: Option<GatewaySurfacesFn>,
    /// Optional identity extractor ([`super::GatewayOpts::identity`]).
    pub(crate) identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    /// Graceful drain timeout for active connections.
    pub drain_timeout: Duration,
    /// Optional server-only TLS (`TREMBITA_HTTP_TLS_*` / [`super::GatewayOpts::tls`]).
    pub tls: Option<GatewayTlsPaths>,
    /// Optional gateway-wide requests-per-second cap ([`super::GatewayOpts::rate_limit_per_sec`]).
    pub rate_limit_per_sec: Option<u32>,
}
