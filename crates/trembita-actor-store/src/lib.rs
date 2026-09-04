//! `trembita-actor-store` — workflow state for stateful actors ([`ActorStateStore`]).

mod redb_store;
mod store;
mod store_codec;
mod store_service;

pub use redb_store::{
    DEFAULT_ACTOR_STORE_GC_MAX_KEYS, DEFAULT_ACTOR_STORE_GC_PERIOD, RedbActorStateStore,
    StoreReplicationOps,
};
pub use store::{ActorStateStore, InMemoryStore, StoreError};
pub use store_codec::{store_get, store_set};
pub use store_service::{ClusterActorStateStore, StoreService, run_actor_store_gc_ticker};
pub use trembita_proto::BoxFuture;
