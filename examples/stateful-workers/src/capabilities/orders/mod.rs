//! Orders group — one op per file under this directory.

mod process_order;

use process_order::process_order_handler_register;
use trembita::{cap_register_chain, CapGroup, CapManifest};

pub use process_order::ProcessOrder;

#[derive(Default)]
pub struct OrdersState;

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<OrdersState>::for_cap::<ProcessOrder>()
            .instances(1)
            .default_queue_for::<ProcessOrder>(),
        process_order_handler_register,
    ))
}
