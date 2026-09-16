//! Capability enqueue for the email backlog.

use trembita::{cap_enqueue, RouteTable, TrembitaGatewayState};

use crate::capabilities::email::DeliverEmail;

#[must_use]
pub fn route_table(state: TrembitaGatewayState) -> RouteTable {
    RouteTable::new().post(
        "/jobs/emails",
        cap_enqueue::<DeliverEmail>(state),
    )
}
