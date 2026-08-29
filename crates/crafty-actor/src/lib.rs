//! `crafty-actor` — the node runtime that ties consensus, storage, transport,
//! and the actor model together (backlog Wave 2).
//!
//! Hosts the consensus node runtime ([`spawn_node`]), the local
//! [`ActorRegistry`] (E6), and — in later increments — cross-node
//! delivery/routing (cross-node-actors, cluster-routing) and the leader-only `ClusterSupervisor`
//! (supervisor-leader).

pub use {crafty_core, crafty_net, crafty_proto, crafty_storage};

/// Attribute macro that fills in the `postcard` wire codecs on a [`UserActor`]
/// `impl` so the actor is remotely spawnable and addressable (cross-node-actors). See the
/// [macro docs](macro@remote_actor) for usage.
pub use crafty_macros::remote_actor;

mod directory;
mod directory_policy;
mod driver;
mod group_membership;
mod group_rebalance;
mod mailbox_spool;
mod messaging;
mod meta;
mod placement;
mod queue;
mod queue_autoscale;
mod queue_schedule;
mod queue_service;
mod redb_queue;
mod redb_store;
mod registry;
mod resources;
mod ring;
mod runtime;
mod session;
mod sharded;
mod sharded_queue;
mod store;
mod store_codec;
mod store_service;
mod supervisor;
mod tracing_init;
mod two_phase;

pub use directory::{ActorDirectory, ClusterRef, DirectorySync};
pub use directory_policy::{DirectoryPolicy, DirectoryRetry};
pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use group_membership::{GroupMembershipSyncReport, sync_hosted_group_membership};
pub use group_rebalance::{GroupRebalanceReport, RaftGroupReconciler};
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
pub(crate) use queue::after_failed_attempt;
pub use queue::{
    EnqueueOptions, InMemoryJobQueue, JobId, JobLifecycle, JobQueue, JobStatus, LeaseId, LeasedJob,
    QueueError, QueueMetrics, QueueReplicateOp, QueueReplicationOps, WorkerId, run_queue_consumer,
};
pub use queue_autoscale::{
    AutoscalePolicy, MembershipAutoscalePolicy, QueueAutoscaleRegistry, run_queue_autoscaler,
    run_queue_membership_autoscaler,
};
pub use queue_schedule::{
    RecurringJob, parse_cron, run_queue_schedule_ticker, run_recurring_job_ticker,
};
pub use queue_service::{ClusterJobQueue, QueueService};
pub use redb_queue::RedbJobQueue;
pub use redb_store::{RedbActorStateStore, StoreReplicationOps};
pub use registry::{
    ASK_TIMEOUT, ActorGroupStats, ActorObserver, ActorRef, ActorRegistry, AskError,
    ConfigCodecError, DEFAULT_DRAIN_TIMEOUT, DeliverError, DrainOutcome, MessageDecodeError,
    MigrationError, PlacementMode, PoolRef, RestartPolicy, RpcReplyPort, ScaleError, SendError,
    SnapshotError, SpawnError, StopError, UserActor, WireReplyPort,
};
pub use resources::{ResourceProfile, VpsResources};
pub use ring::{VIRTUAL_NODES, group_salt, hash_bytes, hash_key as ring_hash_key, pick_index};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, QueueAutoscalePolicyAppliedFn, RuntimeConfig,
    SagaJournalAppliedFn, TwoPhaseGcAbortedFn, TwoPhaseJournalAppliedFn, spawn as spawn_node,
};
pub use session::ActorSession;
pub use sharded_queue::{ShardedJobQueue, ShardedReplication};
pub use tracing_init::init_tracing;
pub mod rebalance_log;
pub use sharded::{
    MultiRaftSpawnResult, ShardedNodeService, spawn_multi_raft_node, spawn_raft_group,
    spawn_raft_group_from_bundle,
};
pub use store::{ActorStateStore, BoxFuture, InMemoryStore, StoreError};
pub use store_codec::{store_get, store_set};
pub use store_service::{ClusterActorStateStore, StoreService};
pub use supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
