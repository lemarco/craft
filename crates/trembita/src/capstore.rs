//! Capability workflow store — idempotency keys, handler markers, step progress.
//!
//! Product name: **`CapStore`** (crate rename target: **`trembita-capstore`**).
//! Low-level module [`actor_store`](crate::actor_store) keeps the historical `ActorStateStore` name.

pub use trembita_actor_store::{
    ActorStateStore as CapStore, InMemoryStore, RedbActorStateStore as RedbCapStore,
    StoreError as CapStoreError, store_get, store_set,
};
