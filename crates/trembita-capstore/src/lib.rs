//! `trembita-capstore` — durable workflow keys for capabilities ([`CapStateStore`]).

mod redb_store;
mod store;
mod store_codec;
mod store_service;

pub use redb_store::{
    DEFAULT_CAP_STORE_GC_MAX_KEYS, DEFAULT_CAP_STORE_GC_PERIOD, RedbCapStateStore,
    StoreReplicationOps,
};
pub use store::{CapStateStore, InMemoryStore, StoreError};
pub use store_codec::{store_get, store_set};
pub use store_service::{ClusterCapStateStore, StoreService, run_cap_store_gc_ticker};
pub use trembita_proto::BoxFuture;

#[deprecated(since = "0.6.1", note = "renamed to ClusterCapStateStore")]
pub use ClusterCapStateStore as ClusterActorStateStore;
#[deprecated(since = "0.6.1", note = "renamed to DEFAULT_CAP_STORE_GC_MAX_KEYS")]
pub use DEFAULT_CAP_STORE_GC_MAX_KEYS as DEFAULT_ACTOR_STORE_GC_MAX_KEYS;
#[deprecated(since = "0.6.1", note = "renamed to DEFAULT_CAP_STORE_GC_PERIOD")]
pub use DEFAULT_CAP_STORE_GC_PERIOD as DEFAULT_ACTOR_STORE_GC_PERIOD;
#[deprecated(since = "0.6.1", note = "renamed to RedbCapStateStore")]
pub use RedbCapStateStore as RedbActorStateStore;
#[deprecated(since = "0.6.1", note = "renamed to run_cap_store_gc_ticker")]
pub use run_cap_store_gc_ticker as run_actor_store_gc_ticker;
#[deprecated(since = "0.6.1", note = "renamed to CapStateStore")]
pub use store::CapStateStore as ActorStateStore;
