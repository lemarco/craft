//! `ActorRegistry` — local actor spawn / pool / scale / stop (backlog E6,
//! [cluster-elasticity](../../../docs/decisions/cluster-elasticity.md),
//! [cluster-elasticity#one-worker-per-vps-production](../../../docs/decisions/cluster-elasticity.md#one-worker-per-vps-production)).
//!
//! This is the **local** half of the actor fabric: named singletons and pools
//! of user actors running on the node, with round-robin and keyed message
//! routing. Cross-node addressing, the cluster directory, and remote
//! spawn/scale (cross-node-actors, cluster-routing) layer on top of these primitives in later
//! increments (E7–E9); the API here is shaped so they can.
//!
//! ## Actor model
//!
//! A [`UserActor`] owns some state built from a `Config` and handles one
//! `Message` at a time on its own tokio task (a serial mailbox — no interior
//! locking needed in user code). Request/response ("ask") is expressed by
//! carrying an [`RpcReplyPort`] inside a message, exactly like `ractor`'s
//! `RpcReplyPort`, so a single `Message` type covers both fire-and-forget and
//! call semantics.
//!
//! Like the node runtime (E1), this is built directly on tokio rather than an
//! external actor framework, keeping the dependency surface small and the whole
//! thing deterministic and unit-testable.
//!
//! ## Production vs development (one-worker-per-vps)
//!
//! Production runs **one worker per VPS per name**: [`spawn_pool`] and
//! [`scale_local`] with a count `> 1` are rejected unless the registry is built
//! with [`ActorRegistry::new_dev`]. Scale out by adding VPSes (E9
//! `scale_cluster`), not by stacking workers locally.
//!
//! [`spawn_pool`]: ActorRegistry::spawn_pool
//! [`scale_local`]: ActorRegistry::scale_local

mod actor;
mod errors;
mod inner;
mod lifecycle;
mod observer;
mod pool;
mod refs;
mod reply;

pub use actor::UserActor;
pub use errors::{
    AskError, ConfigCodecError, DeliverError, DrainOutcome, MessageDecodeError, MigrationError,
    RestartPolicy, ScaleError, SendError, SnapshotError, SpawnError, StopError,
};
pub use inner::{ActorRegistry, PlacementMode};
pub use observer::{ActorGroupStats, ActorObserver, LocalActorIntrospection};
pub use refs::{ActorRef, PoolRef};
pub use reply::{RpcReplyPort, WireReplyPort};

/// Default graceful-drain timeout for stopping/migrating an actor instance
/// ([drain-timeout](../../../docs/decisions/drain-timeout.md)). Overridable per
/// call; the facade exposes `.drain_timeout(..)` / `TREMBITA_DRAIN_TIMEOUT`.
pub const DEFAULT_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Caller-side deadline for `ask` (request/reply): if the target actor does not
/// answer within this window, [`ActorRef::ask`] / [`PoolRef::ask`] return
/// [`AskError::Timeout`] instead of blocking forever on a wedged or slow
/// handler. Mirrors the cross-node ask deadline so local and remote asks bound
/// the caller symmetrically.
pub const ASK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
