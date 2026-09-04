//! Internal builder spec types and override flags.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use trembita_events::{
    EventOutboxDrainOpts, EventOutboxSource, TopicRetentionOpts, TopicSubscriptionDef,
};
use trembita_jobs::{
    BacklogFeedOpts, BacklogRegistry, ExternalBacklog, JobQueue, QueueAutoscaleRegistry,
    RecurringJob, ScheduleSource,
};
use trembita_proto::QueueAutoscalePolicyCommand;
use trembita_runtime::{ActorDirectory, ClusterControl, ClusterState, LeaderGate, LeaderLoopOpts};

/// Type-erased "register this actor type on the control plane" step.
pub(super) type RegisterFn = Box<dyn FnOnce(&ClusterControl) + Send>;
/// Type-erased "declare this managed group on the supervisor" step.
pub(super) type ManageFn = Box<
    dyn FnOnce(&trembita_runtime::ClusterSupervisor<Arc<crate::cluster_handle::ClusterFacts>>)
        + Send,
>;
/// Type-erased user leader task registered via [`super::TrembitaClusterBuilder::on_leader`].
pub(super) type UserLeaderTask =
    Arc<dyn Fn(LeaderGate) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub(super) struct UserLeaderTaskSpec {
    pub opts: LeaderLoopOpts,
    pub tick: UserLeaderTask,
}

/// Type-erased queue autoscale background task spawned at node start.
pub(super) type AutoscaleTask = Box<
    dyn FnOnce(
            Arc<ClusterControl>,
            Arc<dyn ClusterState>,
            Arc<ActorDirectory>,
            std::collections::HashMap<String, Arc<dyn JobQueue>>,
            Arc<QueueAutoscaleRegistry>,
            Arc<BacklogRegistry>,
        ) -> tokio::task::JoinHandle<()>
        + Send,
>;

#[derive(Debug, Clone)]
pub(super) struct JobStreamSpec {
    pub name: String,
    pub path: Option<PathBuf>,
    pub lease_timeout: Duration,
    pub prefetch: usize,
    pub default_max_attempts: u32,
}

#[derive(Debug, Clone)]
pub(super) struct TopicStreamSpec {
    pub name: String,
    pub path: Option<PathBuf>,
    pub lease_timeout: Duration,
    pub retention: TopicRetentionOpts,
    pub subscriptions: Vec<TopicSubscriptionDef>,
}

#[derive(Debug, Clone)]
pub(super) struct ShardedJobSpec {
    pub name: String,
    pub shard_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct RecurringJobSpec {
    pub stream: String,
    pub job: RecurringJob,
}

/// Type-erased membership autoscale background task spawned at node start.
pub(super) type MembershipAutoscaleTask = Box<
    dyn FnOnce(
            Arc<dyn ClusterState>,
            std::collections::HashMap<String, Arc<dyn JobQueue>>,
            Arc<QueueAutoscaleRegistry>,
            Arc<BacklogRegistry>,
        ) + Send,
>;

#[derive(Clone)]
pub(super) struct BacklogFeedSpec {
    pub stream: String,
    pub backlog: Arc<dyn ExternalBacklog>,
    pub opts: BacklogFeedOpts,
}

#[derive(Clone)]
pub(super) struct ScheduleSourceSpec {
    pub stream: String,
    pub source: Arc<dyn ScheduleSource>,
    pub poll: Duration,
}

#[derive(Clone)]
pub(super) struct EventOutboxFeedSpec {
    pub topic: String,
    pub source: Arc<dyn EventOutboxSource>,
    pub opts: EventOutboxDrainOpts,
}

/// Builder options set explicitly in code (not derived from env merge).
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BuilderOverrides {
    pub node_id: bool,
    pub members: bool,
    pub allow_join: bool,
    pub allow_voter_join: bool,
    pub join_role: bool,
    pub allow_leave: bool,
    pub voter_replacement: bool,
    pub voter_replacement_grace_ticks: bool,
    pub drain_timeout: bool,
    pub cert_watch: bool,
}
