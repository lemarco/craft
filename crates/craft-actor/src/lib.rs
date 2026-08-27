//! `craft-actor` — the node runtime that ties consensus, storage, transport,
//! and the actor model together (backlog Wave 2).
//!
//! Hosts the consensus node runtime ([`spawn_node`]), the local
//! [`ActorRegistry`] (E6), and — in later increments — cross-node
//! delivery/routing (ADR 013, ADR 019) and the leader-only `ClusterSupervisor`
//! (ADR 018).

pub use {craft_core, craft_net, craft_proto, craft_storage};

/// Attribute macro that fills in the `postcard` wire codecs on a [`UserActor`]
/// `impl` so the actor is remotely spawnable and addressable (ADR 013). See the
/// [macro docs](macro@remote_actor) for usage.
pub use craft_macros::remote_actor;

mod directory;
mod driver;
mod messaging;
mod placement;
mod registry;
mod resources;
mod runtime;
mod sharded;
mod store;
mod supervisor;

pub use directory::{ActorDirectory, ClusterRef, DirectorySync};
pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use messaging::{AskError as ClusterAskError, CastError, ClusterMessaging};
pub use placement::{
    ClusterControl, ClusterScaleError, MigrateError, NOT_LEADER_REASON, RemoteSpawnError,
    ScalePlan, plan_scale,
};
pub use registry::{
    ASK_TIMEOUT, ActorGroupStats, ActorObserver, ActorRef, ActorRegistry, AskError,
    ConfigCodecError, DEFAULT_DRAIN_TIMEOUT, DeliverError, DrainOutcome, MessageDecodeError,
    MigrationError, PlacementMode, PoolRef, RestartPolicy, RpcReplyPort, ScaleError, SendError,
    SnapshotError, SpawnError, StopError, UserActor, WireReplyPort,
};
pub use resources::{ResourceProfile, VpsResources};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, RuntimeConfig, spawn as spawn_node,
};
pub use sharded::{ShardedNodeService, spawn_multi_raft_node};
pub use store::{ActorStateStore, BoxFuture, InMemoryStore, StoreError};
pub use supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
