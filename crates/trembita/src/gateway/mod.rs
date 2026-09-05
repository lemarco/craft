//! Product HTTP / WebSocket gateway for [`TrembitaApp`](super::app::TrembitaApp).
//!
//! Declare host surfaces with [`GatewayOpts::surfaces`] — see
//! [gateway-routing-v2](../../docs/decisions/gateway-routing-v2.md).

mod config;
mod drain;
mod identity;
mod opts;
mod rate_limit;
mod router;
mod session;
mod spawn;
mod state;

#[cfg(all(test, feature = "http-jobs"))]
mod tests;

use trembita_dashboard::AdminTlsPaths;

/// PEM paths for server-only TLS on the product gateway (HTTPS / WSS).
pub type GatewayTlsPaths = AdminTlsPaths;

pub use config::{
    DEFAULT_CONSUMER_DRAIN_TIMEOUT, DEFAULT_GATEWAY_DRAIN_TIMEOUT, GATEWAY_MAX_BODY_BYTES,
    GatewayConfig, GatewayConfigError, gateway_has_product_apis, gateway_token_from_env,
    validate_gateway_config,
};
pub use drain::{ConnectionGuard, ConnectionTracker, GatewayHandle};
pub use identity::{
    ExtractedIdentity, GatewayBearerIdentity, GatewayIdentity, GatewayRequest,
    GatewayTokenIdentity, IdentityError, IdentityTypeError, SessionKey,
};
pub use opts::GatewayOpts;
pub use router::{
    WrappedGatewayService, bearer_auth_from_env, build_gateway_router, build_gateway_service,
};
pub use session::{NoWorkerError, OpenActorSessionError, SessionHandle};
pub use spawn::{GatewaySpawnError, spawn_gateway};
pub use state::TrembitaGatewayState;
