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
/// [macro docs](macro@actor) for usage.
pub use crafty_macros::actor;

mod backlog_settle_outbox;
mod compute_token;
mod directory;
mod directory_policy;
mod driver;
mod external_backlog;
mod group_membership;
mod group_rebalance;
mod mailbox_spool;
mod messaging;
mod meta;
mod placement;
mod queue;
mod queue_autoscale;
mod queue_lifecycle;
mod queue_prefetch;
mod queue_schedule;
mod queue_service;
mod redb_queue;
mod redb_store;
mod redb_topic;
mod registry;
mod resources;
mod ring;
mod runtime;
mod schedule_source;
mod session;
mod sharded;
mod sharded_queue;
mod store;
mod store_codec;
mod store_service;
mod supervisor;
mod topic;
mod topic_service;
mod tracing_init;
mod two_phase;
mod workload;

pub use backlog_settle_outbox::{
    BacklogSettleOutbox, BacklogSettleOutboxError, BacklogSettleOutboxId, BacklogSettleOutboxOpts,
    InMemoryBacklogSettleOutbox, RedbBacklogSettleOutbox, push_backlog_settle,
};
pub use compute_token::{ComputeGuard, ComputeTokenPool, with_compute_guard};
pub use directory::{ActorDirectory, ClusterRef, DirectorySync};
pub use directory_policy::{DirectoryPolicy, DirectoryRetry};
pub use driver::{DriverError, NetEffect, RaftDriver, ReadOutcome, Step};
pub use external_backlog::{
    BacklogError, BacklogFeedOpts, BacklogItem, BacklogRegistry, BacklogSettleEvent,
    BacklogSettleOutcome, ExternalBacklog, InMemoryExternalBacklog, Settlement,
    effective_queue_depth, emit_backlog_settle_for_terminal_ops, run_backlog_feeder,
    run_backlog_settle_drainer, terminal_backlog_outcome,
};
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
    BatchRequeueResult, EnqueueOptions, InMemoryJobQueue, JobContext, JobId, JobLifecycle,
    JobListFilter, JobListPage, JobQueue, JobStatus, LIST_JOBS_DEFAULT_LIMIT, LeaseId, LeasedJob,
    QueueConsumerWorkload, QueueError, QueueMetrics, QueueReplicateOp, QueueReplicationOps,
    WorkerId, job_status_matches_filter, run_queue_consumer,
};
pub use queue_autoscale::{
    AutoscalePolicy, MembershipAutoscalePolicy, QueueAutoscaleRegistry, run_queue_autoscaler,
    run_queue_membership_autoscaler,
};
pub use queue_lifecycle::QueueLifecycleEvent;
pub use queue_prefetch::{DEFAULT_QUEUE_BATCH_MAX, DEFAULT_QUEUE_PREFETCH};
pub use queue_schedule::{
    RecurringJob, parse_cron, run_queue_schedule_ticker, run_recurring_job_ticker,
};
pub use queue_service::{ClusterJobQueue, QueueService};
pub use redb_queue::RedbJobQueue;
pub use redb_store::{
    DEFAULT_ACTOR_STORE_GC_MAX_KEYS, DEFAULT_ACTOR_STORE_GC_PERIOD, RedbActorStateStore,
    StoreReplicationOps,
};
pub use redb_topic::RedbEventTopic;
pub use registry::{
    ASK_TIMEOUT, ActorGroupStats, ActorObserver, ActorRef, ActorRegistry, AskError,
    ConfigCodecError, DEFAULT_DRAIN_TIMEOUT, DeliverError, DrainOutcome, LocalActorIntrospection,
    MessageDecodeError, MigrationError, PlacementMode, PoolRef, RestartPolicy, RpcReplyPort,
    ScaleError, SendError, SnapshotError, SpawnError, StopError, UserActor, WireReplyPort,
};
pub use resources::{ResourceProfile, VpsResources};
pub use ring::{VIRTUAL_NODES, group_salt, hash_bytes, hash_key as ring_hash_key, pick_index};
pub use runtime::{
    ClientError, NodeHandle, NodeService, NodeStatus, QueueAutoscalePolicyAppliedFn, RuntimeConfig,
    SagaJournalAppliedFn, TwoPhaseGcAbortedFn, TwoPhaseJournalAppliedFn, spawn as spawn_node,
};
pub use schedule_source::{
    CompositeScheduleSource, ScheduleError, SchedulePoll, ScheduleReconcilePlan, ScheduleSource,
    StaticScheduleSource, plan_schedule_reconcile, wire_to_recurring_job,
};
pub use session::ActorSession;
pub use sharded_queue::{ShardedJobQueue, ShardedReplication};
pub use topic::{
    EventId, EventTopic, InMemoryEventTopic, LeasedEvent, SubscriptionStart, TopicContext,
    TopicError, TopicLeaseId, TopicMetrics, TopicReplicationOps, TopicRetentionOpts,
    TopicSubscriptionDef, TopicSubscriptionMetrics, run_topic_subscriber,
};
pub use topic_service::{ClusterEventTopic, TopicService};
pub use tracing_init::init_tracing;
pub use workload::{
    ConsumerTune, WorkloadMetricsHook, WorkloadMetricsSnapshot, WorkloadOpts, run_workload_governor,
};
pub mod rebalance_log;
pub use sharded::{
    MultiRaftSpawnResult, ShardedNodeService, spawn_multi_raft_node, spawn_raft_group,
    spawn_raft_group_from_bundle,
};
pub use store::{ActorStateStore, BoxFuture, InMemoryStore, StoreError};
pub use store_codec::{store_get, store_set};
pub use store_service::{ClusterActorStateStore, StoreService, run_actor_store_gc_ticker};
pub use supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
