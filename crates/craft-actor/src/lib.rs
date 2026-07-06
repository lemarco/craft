//! `craft-actor` — the node runtime that ties consensus, storage, transport,
//! and the actor model together (backlog Wave 2).
//!
//! Hosts the consensus node runtime ([`spawn_node`]), the local
//! [`ActorRegistry`] (E6), and — in later increments — cross-node
//! delivery/routing (ADR 013, ADR 019) and the leader-only `ClusterSupervisor`
//! (ADR 018).

pub use {craft_core, craft_net, craft_proto, craft_storage};

mod directory;
mod driver;
mod messaging;
mod placement;
mod registry;
mod runtime;
mod supervisor;

pub use directory::{ActorDirectory, ClusterRef, DirectorySync};
pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use messaging::{CastError, ClusterMessaging};
pub use placement::{
    ClusterControl, ClusterScaleError, MigrateError, RemoteSpawnError, ScalePlan, plan_scale,
};
pub use registry::{
    ActorRef, ActorRegistry, AskError, ConfigCodecError, DEFAULT_DRAIN_TIMEOUT, DeliverError,
    DrainOutcome, MessageDecodeError, MigrationError, PoolRef, RpcReplyPort, ScaleError, SendError,
    SnapshotError, SpawnError, StopError, UserActor,
};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, RuntimeConfig, spawn as spawn_node,
};
pub use supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
