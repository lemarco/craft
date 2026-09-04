//! `trembita-jobs` — durable job queue port, redb adapter, and leader [`QueueService`].

mod backlog_settle_outbox;
mod external_backlog;
mod queue;
mod queue_autoscale;
mod queue_lifecycle;
mod queue_prefetch;
mod queue_schedule;
mod queue_service;
mod redb_queue;
mod schedule_source;
mod sharded_queue;
mod workload;

pub use backlog_settle_outbox::{
    BacklogSettleOutbox, BacklogSettleOutboxError, BacklogSettleOutboxId, BacklogSettleOutboxOpts,
    InMemoryBacklogSettleOutbox, RedbBacklogSettleOutbox, push_backlog_settle,
};
pub use external_backlog::{
    BacklogError, BacklogFeedOpts, BacklogItem, BacklogRegistry, BacklogSettleEvent,
    BacklogSettleOutcome, ConsumerCount, ExternalBacklog, InMemoryExternalBacklog, Settlement,
    effective_queue_depth, emit_backlog_settle_for_terminal_ops, run_backlog_feeder,
    run_backlog_settle_drainer, terminal_backlog_outcome,
};
pub use queue::{
    BatchRequeueResult, EnqueueOptions, InMemoryJobQueue, JobContext, JobId, JobLifecycle,
    JobListFilter, JobListPage, JobQueue, JobStatus, LIST_JOBS_DEFAULT_LIMIT, LeaseId, LeasedJob,
    QueueConsumerWorkload, QueueError, QueueMetrics, QueueReplicationOps,
    job_status_matches_filter, run_queue_consumer,
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
pub use schedule_source::{
    CompositeScheduleSource, ScheduleError, SchedulePoll, ScheduleReconcilePlan, ScheduleSource,
    StaticScheduleSource, plan_schedule_reconcile, wire_to_recurring_job,
};
pub use sharded_queue::{ShardedJobQueue, ShardedReplication};
pub use trembita_proto::WorkerId;
pub use trembita_runtime::{AttemptOutcome, after_failed_attempt};
pub use workload::{
    ConsumerTune, WorkloadMetricsHook, WorkloadMetricsSnapshot, WorkloadOpts, run_workload_governor,
};
