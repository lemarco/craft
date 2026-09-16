//! Process one order idempotently ([`CapStore`](trembita::capstore::CapStore) marker).

use std::env;

use serde::{Deserialize, Serialize};
use trembita::capstore::{store_get, store_set};
use trembita::{cap_handler, CapError, OpCtx};

use super::OrdersState;
use crate::debug;
use crate::domain::orders::{ProcessOutcome, process_order};

/// Marker stored under `order:{id}` — presence means "already handled".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderDone {
    pub processed: bool,
}

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
    let store = ctx.require_store()?;

    let key = format!("order:{}", msg.order_id);
    let already = store_get::<OrderDone>(&*store, &key)
        .await
        .map_err(CapError::handler)?
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
    .map_err(CapError::handler)?;
    debug::order_handle(msg.order_id, false);
    println!(
        "[orders node {}] order {}: processed → cap store (capability)",
        env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into()),
        msg.order_id
    );
    Ok(ProcessAck { skipped: false })
}
