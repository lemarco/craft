//! Idempotent [`OrderProcessor`] — stateful actor with [`ActorStateStore`].
//!
//! ## Why not Raft propose?
//!
//! Order-processing progress keys change often; stuffing them into the StateMachine
//! log would replicate every retry. `ActorStateStore` gives **per-actor durable
//! keys** in `actor-store.redb` without consensus on each idempotency check.
//!
//! ## Why not the job queue?
//!
//! We need to address **this specific actor** (or a sticky session to it). Casts
//! route to the supervisor-placed `orders` instance; duplicate casts with the same
//! order id must be safe (payment webhooks, client retries).

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use trembita::actor_store::{store_get, store_set};
use trembita::runtime::{MessageDecodeError, UserActor, actor};
use serde::{Deserialize, Serialize};

/// Marker stored under `order:{id}` — presence means "already handled".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderDone {
    pub processed: bool,
}

/// Passed to `UserActor::start` — each node gets its own `data_dir` in cluster mode.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcessorCfg {
    pub data_dir: PathBuf,
}

pub struct OrderProcessor {
    store: Arc<dyn trembita::actor_store::ActorStateStore>,
}

#[derive(Debug)]
pub struct ProcessorErr;
impl std::fmt::Display for ProcessorErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("processor error")
    }
}
impl std::error::Error for ProcessorErr {}

#[actor]
impl UserActor for OrderProcessor {
    type Config = ProcessorCfg;
    type Message = String; // JSON `{"payload":"<order-id>"}` from gateway cast API
    type Error = ProcessorErr;

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        trembita::proto::decode(payload).or_else(|_| {
            std::str::from_utf8(payload)
                .map(|s| s.to_string())
                .map_err(|e| MessageDecodeError::Decode(e.to_string()))
        })
    }

    fn start(cfg: Self::Config) -> Result<Self, Self::Error> {
        std::fs::create_dir_all(&cfg.data_dir).map_err(|_| ProcessorErr)?;
        // redb file colocated with node's TREMBITA_DATA_DIR — survives process restart.
        let store = trembita::actor_store::RedbActorStateStore::open(&cfg.data_dir.join("actor-store.redb"))
            .map_err(|_| ProcessorErr)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    async fn handle(&mut self, raw: Self::Message) -> Result<(), Self::Error> {
        let order_id: u64 = raw.trim().parse().map_err(|_| ProcessorErr)?;
        let key = format!("order:{order_id}");

        // Idempotency: second cast with same order_id is a no-op (client retry / double-click).
        if store_get::<OrderDone>(&*self.store, &key)
            .await
            .map_err(|_| ProcessorErr)?
            .is_some()
        {
            crate::debug::order_handle(order_id, true);
            println!(
                "[orders node {}] order {order_id}: idempotent skip (already in store)",
                env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into())
            );
            return Ok(());
        }

        // … real work would happen here (charge card, emit event, etc.) …

        store_set(
            &*self.store,
            &key,
            &OrderDone { processed: true },
            None, // no TTL — processed orders stay deduplicated forever
        )
        .await
        .map_err(|_| ProcessorErr)?;
        crate::debug::order_handle(order_id, false);
        println!(
            "[orders node {}] order {order_id}: processed → ActorStateStore",
            env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into())
        );
        Ok(())
    }
}
