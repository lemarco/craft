//! App-specific HTTP routes — merged into the default gateway via `.gateway_routes()`.

use trembita::{ProductRoutes, Route, TrembitaGatewayState, cap_invoke};
use trembita_http::RouteTable;

use crate::capabilities::ping::Ping;

/// Custom product routes (webhooks, BFF handlers, capability HTTP, …).
#[must_use]
pub fn route_table(state: TrembitaGatewayState) -> RouteTable {
    ProductRoutes::new()
        // trembita:product-routes
        .post("/ping", cap_invoke::<Ping>(state, Route::Inline))
        // trembita:product-routes-end
        .build()
}
