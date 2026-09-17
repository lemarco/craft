//! Operational routes — health, readiness, metrics, dashboard, introspection.

use trembita::TrembitaGatewayState;
use trembita::RouteTable;

/// Ops route table (`/health`, `/ready`, `/metrics`, `/dashboard`, `/introspect/*`).
#[allow(dead_code)] // optional brownfield merge — default gateway mounts ops automatically
#[must_use]
pub fn route_table(state: &TrembitaGatewayState) -> RouteTable {
    state.app.ops_api().route_table()
}
