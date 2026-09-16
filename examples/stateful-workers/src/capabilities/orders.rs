//! Orders capability — idempotent processing via [`ActorStateStore`].

use std::env;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use trembita::actor_store::{store_get, store_set};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};
use trembita_tools::showcase_common::data_dir;

use crate::debug;

const DATA_DIR_NAME: &str = "trembita-showcase-stateful-workers";

/// Marker stored under `order:{id}` — presence means "already handled".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderDone {
    pub processed: bool,
}

/// Shared state for the `orders` capability host (store opened once per instance).
#[derive(Default)]
pub struct OrdersState {
    store: Option<Arc<dyn trembita::actor_store::ActorStateStore>>,
}

impl OrdersState {
    fn store(&mut self) -> Result<Arc<dyn trembita::actor_store::ActorStateStore>, CapError> {
        if self.store.is_none() {
            let dir = data_dir(DATA_DIR_NAME);
            std::fs::create_dir_all(&dir).map_err(|e| CapError::Handler(e.to_string()))?;
            let path = dir.join("actor-store.redb");
            let store = trembita::actor_store::RedbActorStateStore::open(&path)
                .map_err(|e| CapError::Handler(e.to_string()))?;
            self.store = Some(Arc::new(store));
        }
        Ok(Arc::clone(self.store.as_ref().expect("store init")))
    }
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
async fn process_order(
    msg: ProcessOrder,
    state: &mut OrdersState,
) -> Result<ProcessAck, CapError> {
    let store = state.store()?;
    let key = format!("order:{}", msg.order_id);

    if store_get::<OrderDone>(&*store, &key)
        .await
        .map_err(|e| CapError::Handler(e.to_string()))?
        .is_some()
    {
        debug::order_handle(msg.order_id, true);
        println!(
            "[orders node {}] order {}: idempotent skip (capability)",
            env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into()),
            msg.order_id
        );
        return Ok(ProcessAck { skipped: true });
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
        CapGroup::<OrdersState>::for_cap::<ProcessOrder>().instances(1),
        process_order_register,
    ))
}
