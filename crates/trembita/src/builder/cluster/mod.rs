//! [`TrembitaClusterBuilder`] — the single ergonomic entry point (deployment-model,
//! library-and-publishing). Describe a node (its id, membership, state machine, actor types, and
//! managed groups), then `start_*` it over a transport; the builder assembles
//! the consensus runtime, the actor control/messaging/directory planes, the
//! leader-only supervisor, telemetry, and the admin server, and wires the
//! background loops that keep them current.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use trembita_core::{Config, StateMachine};
use trembita_dashboard::{AdminTlsPaths, MetricsSink};
use trembita_jobs::WorkloadOpts;
use trembita_net::TrafficPolicy;
use trembita_proto::{JoinRole, NodeId, QueueAutoscalePolicyCommand};
use trembita_runtime::{DirectoryPolicy, DirectoryRetry, ResourceProfile, RuntimeConfig};

use crate::discovery::Seed;

mod assemble;
mod config;
mod products;
mod start;
mod topic_leader;
mod types;

#[cfg(all(test, feature = "dev-certs"))]
mod tests;

use types::{
    AutoscaleTask, BacklogFeedSpec, BuilderOverrides, EventOutboxFeedSpec, JobStreamSpec, ManageFn,
    MembershipAutoscaleTask, RecurringJobSpec, RegisterFn, ScheduleSourceSpec, ShardedJobSpec,
    TopicStreamSpec, UserLeaderTaskSpec,
};

/// A fluent builder for a single trembita node (deployment-model). Create it with
/// [`TrembitaCluster::builder`](crate::cluster::TrembitaCluster::builder).
pub struct TrembitaClusterBuilder<M: StateMachine> {
    node_id: NodeId,
    machine: M,
    members: Vec<NodeId>,
    raft: Config,
    runtime: RuntimeConfig,
    dev_multi_workers: bool,
    resource_profile: ResourceProfile,
    forward_timeout: Duration,
    reconcile_period: Duration,
    publish_period: Duration,
    refresh_period: Duration,
    event_capacity: usize,
    metrics_sink: Option<Arc<dyn MetricsSink>>,
    admin_addr: Option<SocketAddr>,
    admin_tls: Option<AdminTlsPaths>,
    join_seeds: Vec<Seed>,
    /// Role requested on dynamic join ([`Self::join_as`]).
    join_role: JoinRole,
    traffic_policy: TrafficPolicy,
    raft_groups: u32,
    shard_count: u32,
    shard_routing: trembita_core::ShardRoutingKind,
    group_replication_factor: u32,
    group_learner_factor: u32,
    raft_machines: Option<Vec<M>>,
    data_dir: Option<PathBuf>,
    actor_state_store: Option<Arc<dyn trembita_actor_store::ActorStateStore>>,
    /// Open `{data_dir}/actor-store.redb` with voter replication when no explicit store is set.
    auto_durable_actor_store: bool,
    drain_timeout: Duration,
    directory_policy: DirectoryPolicy,
    directory_retry: DirectoryRetry,
    /// Poll interval for on-disk PEM rotation when using [`start_quic_pem`](Self::start_quic_pem).
    cert_watch: Option<Duration>,
    registrations: Vec<RegisterFn>,
    managed: Vec<ManageFn>,
    job_streams: Vec<JobStreamSpec>,
    topic_streams: Vec<TopicStreamSpec>,
    job_sharded: Vec<ShardedJobSpec>,
    recurring_jobs: Vec<RecurringJobSpec>,
    job_autoscale: Vec<AutoscaleTask>,
    job_membership_autoscale: Vec<MembershipAutoscaleTask>,
    queue_autoscale_meta: BTreeMap<String, QueueAutoscalePolicyCommand>,
    backlog_feeds: Vec<BacklogFeedSpec>,
    schedule_sources: Vec<ScheduleSourceSpec>,
    event_outbox_feeds: Vec<EventOutboxFeedSpec>,
    /// Per-node workload governor ([workload-governor](../../docs/decisions/workload-governor.md)).
    workload: Option<WorkloadOpts>,
    /// Persist cross-node `/actor/deliver` envelopes to redb outbox/inbox.
    durable_mailbox: bool,
    /// User-defined leader-only periodic tasks ([`Self::on_leader`]).
    leader_tasks: Vec<UserLeaderTaskSpec>,
    /// Code-first settings that must not be overwritten by env merge.
    overrides: BuilderOverrides,
}
