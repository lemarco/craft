//! `trembita-runtime` — Raft node runtime, actor registry, and cluster supervision.
//!
//! Hosts [`spawn_node`], [`ActorRegistry`], cross-node messaging, and the
//! leader-only [`ClusterSupervisor`].

pub use {trembita_core, trembita_net, trembita_proto, trembita_storage};

/// Attribute macro for remotely spawnable [`UserActor`] wire codecs.
pub use trembita_macros::actor;

mod compute_token;
mod directory;
mod directory_policy;
mod driver;
mod external_load;
mod group_membership;
mod group_rebalance;
mod leader_replicate;
mod leader_task;
mod mailbox_spool;
mod messaging;
mod meta;
mod placement;
pub mod rebalance_log;
mod registry;
mod resources;
mod retry;
mod ring;
mod runtime;
mod session;
mod sharded;
mod supervisor;
mod tracing_init;
mod two_phase;

pub use compute_token::{
    ComputeGuard, ComputeTokenPool, with_compute_guard, with_compute_guard_weighted,
};
pub use directory::{ActorDirectory, ClusterRef, DirectorySync};
pub use directory_policy::{DirectoryPolicy, DirectoryRetry};
pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use external_load::{ExternalLoad, ManualExternalLoad};
pub use group_membership::{GroupMembershipSyncReport, sync_hosted_group_membership};
pub use group_rebalance::{GroupRebalanceReport, RaftGroupReconciler};
pub use leader_replicate::{
    REPLICATION_NO_REACHABLE_VOTERS, authorize_replicate_leader, fanout_product_replicate,
    fanout_replicate, forward_to_leader, replicate_reply_err, replication_peers,
};
pub use leader_task::{LeaderGate, LeaderLoopOpts, LeaderSession, run_leader_loop};
pub use mailbox_spool::{
    InMemoryMailboxSpool, MailboxSpool, MailboxSpoolError, MailboxSpoolId, RedbMailboxSpool,
};
pub use messaging::{
    AskError as ClusterAskError, CastError, ClusterMessaging, run_mailbox_spool_drainer,
};
pub use meta::{MetaCommand, MetaError, MetaQuery, MetaResponse, MetaStateMachine};
pub use placement::{
    ClusterControl, ClusterScaleError, MigrateError, NOT_LEADER_REASON, RemoteSpawnError,
    ScalePlan, plan_scale,
};
pub use registry::{
    ASK_TIMEOUT, ActorGroupStats, ActorObserver, ActorRef, ActorRegistry, AskError,
    ConfigCodecError, DEFAULT_DRAIN_TIMEOUT, DeliverError, DrainOutcome, LocalActorIntrospection,
    MessageDecodeError, MigrationError, PlacementMode, PoolRef, RestartPolicy, RpcReplyPort,
    ScaleError, SendError, SnapshotError, SpawnError, StopError, UserActor, WireReplyPort,
};
pub use resources::{ResourceProfile, VpsResources};
pub use retry::{AttemptOutcome, after_failed_attempt};
pub use ring::{VIRTUAL_NODES, group_salt, hash_bytes, hash_key as ring_hash_key, pick_index};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, QueueAutoscalePolicyAppliedFn, RuntimeConfig,
    SagaJournalAppliedFn, TwoPhaseGcAbortedFn, TwoPhaseJournalAppliedFn, spawn as spawn_node,
};
pub use session::ActorSession;
pub use sharded::{
    MultiRaftSpawnResult, ShardedNodeService, spawn_multi_raft_node, spawn_raft_group,
    spawn_raft_group_from_bundle,
};
pub use supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
pub use tracing_init::init_tracing;
