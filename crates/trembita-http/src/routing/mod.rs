//! Native gateway HTTP routing (trembita 0.4.0).
//!
//! Product apps declare [`RouteTable`] entries instead of Axum routers. See
//! [gateway-routing-v2](../../../docs/decisions/gateway-routing-v2.md).

/// Maximum HTTP request body size (16 MiB — matches gateway cap).
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

mod auth;
mod ctx;
mod error;
mod handler;
mod path;
mod table;

pub use auth::{AuthMode, SessionGate};
pub use ctx::{RequestCtx, Response, ResponseBody};
pub use error::HttpError;
pub use handler::{ArcHandler, Handler};
pub use path::{PathParams, PathPattern, PathSegment};
pub use table::{RouteEntry, RouteTable};
