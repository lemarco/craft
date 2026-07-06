//! `craft-actor` — the node runtime that ties consensus, storage, transport,
//! and the actor model together (backlog Wave 2).
//!
//! Hosts the consensus node runtime ([`spawn_node`]), the local
//! [`ActorRegistry`] (E6), and — in later increments — cross-node
//! delivery/routing (ADR 013, ADR 019) and the leader-only `ClusterSupervisor`
//! (ADR 018).

pub use {craft_core, craft_net, craft_proto, craft_storage};

mod driver;
mod registry;
mod runtime;

pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use registry::{
    ActorRef, ActorRegistry, AskError, PoolRef, RpcReplyPort, ScaleError, SendError, SpawnError,
    StopError, UserActor,
};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, RuntimeConfig, spawn as spawn_node,
};
