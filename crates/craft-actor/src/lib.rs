//! `craft-actor` — the node runtime that ties consensus, storage, transport,
//! and the actor model together (backlog Wave 2).
//!
//! Hosts the consensus node runtime ([`spawn_node`]), the local
//! [`ActorRegistry`] (E6), and — in later increments — cross-node
//! delivery/routing (cross-node-actors, cluster-routing) and the leader-only `ClusterSupervisor`
//! (supervisor-leader).

pub use {craft_core, craft_net, craft_proto, craft_storage};

/// Attribute macro that fills in the `postcard` wire codecs on a [`UserActor`]
/// `impl` so the actor is remotely spawnable and addressable (cross-node-actors). See the
/// [macro docs](macro@remote_actor) for usage.
pub use craft_macros::remote_actor;

mod directory;
mod directory_policy;
mod driver;
mod group_membership;
mod group_rebalance;
mod messaging;
mod meta;
mod placement;
mod registry;
mod resources;
mod ring;
mod runtime;
mod session;
mod sharded;
mod store;
mod store_codec;
mod supervisor;
mod tracing_init;
mod two_phase;

pub use directory::{ActorDirectory, ClusterRef, DirectorySync};
pub use directory_policy::{DirectoryPolicy, DirectoryRetry};
pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use group_membership::{GroupMembershipSyncReport, sync_hosted_group_membership};
pub use group_rebalance::{GroupRebalanceReport, RaftGroupReconciler};
pub use messaging::{AskError as ClusterAskError, CastError, ClusterMessaging};
pub use meta::{MetaCommand, MetaError, MetaQuery, MetaResponse, MetaStateMachine};
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
pub use ring::{VIRTUAL_NODES, group_salt, hash_bytes, hash_key as ring_hash_key, pick_index};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, RuntimeConfig, SagaJournalAppliedFn,
    spawn as spawn_node,
};
pub use session::ActorSession;
pub use tracing_init::init_tracing;
pub mod rebalance_log;
pub use sharded::{
    MultiRaftSpawnResult, ShardedNodeService, spawn_multi_raft_node, spawn_raft_group,
    spawn_raft_group_from_bundle,
};
pub use store::{ActorStateStore, BoxFuture, InMemoryStore, StoreError};
pub use store_codec::{store_get, store_set};
pub use supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
