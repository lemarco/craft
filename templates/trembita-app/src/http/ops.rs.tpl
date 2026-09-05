//! Operational routes — health, readiness, metrics, dashboard, introspection.

use trembita::TrembitaGatewayState;
use trembita_http::RouteTable;

/// Ops route table (`/health`, `/ready`, `/metrics`, `/dashboard`, `/introspect/*`).
#[must_use]
pub fn route_table(state: &TrembitaGatewayState) -> RouteTable {
    state.app.ops_api().route_table()
}
