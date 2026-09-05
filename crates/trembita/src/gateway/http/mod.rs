//! Native gateway HTTP model (trembita 0.4.0).
//!
//! Re-exports from `trembita-http`. See [gateway-routing-v2](https://gitlab.com/lemarco/trembita/-/blob/main/docs/decisions/gateway-routing-v2.md).

#[cfg(feature = "http-jobs")]
pub use trembita_http::{
    ArcHandler, AuthMode, CorsPolicy, Gateway, GatewayBuildError, Handler, HttpError, PathParams,
    PathPattern, PathSegment, RequestCtx, Response, ResponseBody, RouteEntry, RouteTable,
    SessionGate, Surface,
};

#[cfg(not(feature = "http-jobs"))]
compile_error!("trembita::gateway::http requires the `http-jobs` feature");
