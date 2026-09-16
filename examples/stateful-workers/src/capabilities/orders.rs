//! Orders capability — idempotent processing via [`CapStore`](trembita::capstore::CapStore).

use std::env;

use serde::{Deserialize, Serialize};
use trembita::capstore::{store_get, store_set};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest, OpCtx};

use crate::debug;
use crate::domain::orders::{ProcessOutcome, process_order};

/// Marker stored under `order:{id}` — presence means "already handled".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderDone {
    pub processed: bool,
}

#[derive(Default)]
pub struct OrdersState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessOrder {
    pub order_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ProcessAck {
    pub skipped: bool,
}

#[cap_handler(group = "orders", key = "order_id")]
async fn process_order_handler(
    msg: ProcessOrder,
    ctx: OpCtx<'_>,
    _state: &mut OrdersState,
) -> Result<ProcessAck, CapError> {
    let store = ctx
        .cap_store()
        .ok_or_else(|| CapError::Handler("data_dir / cap store required".into()))?;

    let key = format!("order:{}", msg.order_id);
    let already = store_get::<OrderDone>(&*store, &key)
        .await
        .map_err(|e| CapError::Handler(e.to_string()))?
        .is_some();

    match process_order(msg.order_id, already) {
        ProcessOutcome::AlreadyProcessed => {
            debug::order_handle(msg.order_id, true);
            println!(
                "[orders node {}] order {}: idempotent skip (capability)",
                env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into()),
                msg.order_id
            );
            return Ok(ProcessAck { skipped: true });
        }
        ProcessOutcome::Applied => {}
    }

    store_set(
        &*store,
        &key,
        &OrderDone { processed: true },
        None,
    )
    .await
    .map_err(|e| CapError::Handler(e.to_string()))?;
    debug::order_handle(msg.order_id, false);
    println!(
        "[orders node {}] order {}: processed → ActorStateStore (capability)",
        env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into()),
        msg.order_id
    );
    Ok(ProcessAck { skipped: false })
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<OrdersState>::for_cap::<ProcessOrder>()
            .instances(1)
            .default_queue_for::<ProcessOrder>(),
        process_order_handler_register,
    ))
}
