use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;

use super::GatewayTlsPaths;
use super::identity;
use super::state::TrembitaGatewayState;

/// Maximum HTTP request body size on the product gateway (matches QUIC wire cap).
pub const GATEWAY_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Invalid gateway wiring collected at router build / spawn time.
#[derive(Debug, thiserror::Error)]
pub enum GatewayConfigError {
    /// [`super::GatewayOpts::protect_product_apis`] without [`super::GatewayOpts::identity`].
    #[error("protect_product_apis(true) requires GatewayOpts::identity")]
    ProtectApisWithoutIdentity,
    /// Product APIs are enabled but no identity extractor is configured.
    #[error("gateway product APIs require GatewayOpts::identity")]
    ProductApisWithoutIdentity,
}

/// Returns `true` when built-in product routes would be mounted.
#[must_use]
pub fn gateway_has_product_apis(config: &GatewayConfig) -> bool {
    config.jobs_api || config.actors_api || config.workflows_api || config.introspect_api
}

/// Validate gateway wiring before bind.
///
/// # Errors
/// [`GatewayConfigError`] when auth is required but identity is missing.
pub fn validate_gateway_config(config: &GatewayConfig) -> Result<(), GatewayConfigError> {
    if config.protect_apis && config.identity.is_none() {
        return Err(GatewayConfigError::ProtectApisWithoutIdentity);
    }
    if gateway_has_product_apis(config) && config.identity.is_none() {
        return Err(GatewayConfigError::ProductApisWithoutIdentity);
    }
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

/// User-supplied gateway router builder (captures [`TrembitaGatewayState`] in handlers).
pub type GatewayRoutesFn = Box<dyn FnOnce(TrembitaGatewayState) -> Router + Send>;

/// Gateway listen address and route wiring collected on [`super::super::app::TrembitaAppBuilder`].
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
    /// Mount read-only `/introspect/*` routes ([`IntrospectApi`](trembita_http::IntrospectApi)).
    pub introspect_api: bool,
    /// Optional identity extractor ([`super::GatewayOpts::identity`]).
    pub(crate) identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    /// Graceful drain timeout for active connections.
    pub drain_timeout: Duration,
    /// Optional server-only TLS (`TREMBITA_GATEWAY_TLS_*` / [`super::GatewayOpts::tls`]).
    pub tls: Option<GatewayTlsPaths>,
    /// When `true`, built-in product APIs require [`super::GatewayOpts::identity`].
    pub protect_apis: bool,
    /// Optional gateway-wide requests-per-second cap ([`super::GatewayOpts::rate_limit_per_sec`]).
    pub rate_limit_per_sec: Option<u32>,
}
