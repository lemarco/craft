//! Authenticated order submit — primary product HTTP for the orders capability.

use trembita::{ProductRoutes, TrembitaGatewayState, cap_fire};

use crate::capabilities::orders::ProcessOrder;

pub fn order_routes(state: TrembitaGatewayState) -> trembita::RouteTable {
    ProductRoutes::new()
        .post_identity("/orders/submit", cap_fire::<ProcessOrder>(state))
        .build()
}
