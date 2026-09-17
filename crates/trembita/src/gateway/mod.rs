//! Product HTTP / WebSocket gateway for [`TrembitaApp`](super::app::TrembitaApp).
//!
//! Declare host surfaces with [`GatewayOpts::surfaces`] — see
//! [gateway-routing-v2](../../docs/decisions/gateway-routing-v2.md).

mod auth_profile;
#[cfg(feature = "http-jobs")]
mod cap_handlers;
#[cfg(feature = "http-jobs")]
mod cluster_ops;
#[cfg(feature = "http-jobs")]
mod cluster_session;
mod config;
mod drain;
mod identity;
mod opts;
#[cfg(all(feature = "http-jobs", feature = "gateway-session-postgres"))]
mod pg_gateway_session;
#[cfg(feature = "http-jobs")]
mod product_routes;
mod rate_limit;
mod router;
mod session;
mod spawn;
mod state;
#[cfg(feature = "http-jobs")]
pub mod ws;

#[cfg(all(test, feature = "http-jobs"))]
mod tests;

use trembita_dashboard::AdminTlsPaths;

/// PEM paths for server-only TLS on the product gateway (HTTPS / WSS).
pub type GatewayTlsPaths = AdminTlsPaths;

pub use auth_profile::GatewayAuthProfile;
#[cfg(feature = "http-jobs")]
pub use cluster_session::{
    CapStoreGatewaySessionStore, CapStoreSessionIssuer, CapStoreSessionVerifier,
    ClusterSessionError, ClusterSessionSecret, GatewaySessionStore, SignedCookieSessionIssuer,
    SignedCookieSessionVerifier, VerifiedClusterSession, capstore_session_gate,
    cluster_session_gate, opaque_gateway_session_token, register_capstore_session,
    revoke_capstore_session, rotating_cluster_session_gate, session_user_from_cookie,
    session_user_from_verifier, validate_gateway_session_user, verify_capstore_session,
};
pub(crate) use config::GatewaySurfacesFn;
pub use config::{
    DEFAULT_CONSUMER_DRAIN_TIMEOUT, DEFAULT_GATEWAY_DRAIN_TIMEOUT, GATEWAY_MAX_BODY_BYTES,
    GatewayConfig, GatewayConfigError, gateway_token_from_env, validate_gateway_config,
};
pub use drain::{ConnectionGuard, ConnectionTracker, GatewayHandle};
pub use identity::{
    ExtractedIdentity, GatewayBearerIdentity, GatewayIdentity, GatewayRequest,
    GatewayTokenIdentity, IdentityError, IdentityTypeError, SessionKey,
};
pub use opts::GatewayOpts;
pub use router::{WrappedGatewayService, build_gateway_service};
pub use session::{NoWorkerError, OpenWorkerSessionError, SessionHandle};

#[allow(deprecated)]
pub use session::OpenActorSessionError;
pub use spawn::{GatewaySpawnError, spawn_gateway};
pub use state::TrembitaGatewayState;

#[cfg(feature = "http-jobs")]
pub use cap_handlers::{
    CapEnqueueHandler, CapFireHandler, CapInvokeHandler, CapScheduleHandler, cap_enqueue, cap_fire,
    cap_invoke, cap_queued_wait, cap_schedule,
};
#[cfg(feature = "http-jobs")]
pub use cluster_ops::{cluster_ops_route_table, spawn_cluster_ops_http};
#[cfg(all(feature = "http-jobs", feature = "gateway-session-postgres"))]
pub use pg_gateway_session::pg_gateway_session_gate;
#[cfg(feature = "http-jobs")]
pub use product_routes::ProductRoutes;
