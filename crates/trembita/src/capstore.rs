//! Capability workflow store — idempotency keys, handler markers, step progress.

pub use trembita_capstore::{
    CapStateStore as CapStore, InMemoryStore, RedbCapStateStore as RedbCapStore,
    StoreError as CapStoreError, store_get, store_set,
};
