//! App-specific HTTP routes — merged into the default gateway via `.gateway_routes()`.

use trembita::TrembitaGatewayState;
use trembita_http::RouteTable;

/// Custom product routes (webhooks, BFF handlers, …).
#[must_use]
pub fn route_table(_state: &TrembitaGatewayState) -> RouteTable {
    RouteTable::new()
}
