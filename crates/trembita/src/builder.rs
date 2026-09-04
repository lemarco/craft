//! [`TrembitaClusterBuilder`] — the single ergonomic entry point (deployment-model,
//! library-and-publishing). Describe a node (its id, membership, state machine, actor types, and
//! managed groups), then `start_*` it over a transport; the builder assembles
//! the consensus runtime, the actor control/messaging/directory planes, the
//! leader-only supervisor, telemetry, and the admin server, and wires the
//! background loops that keep them current.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use trembita_core::{
    Config, DEFAULT_GROUP_LEARNER_FACTOR, DEFAULT_GROUP_REPLICATION_FACTOR, RaftNode,
    ReachabilityConfig, StateMachine,
};
use trembita_dashboard::{
    AdminServer, AdminTlsPaths, EventBus, Metrics, MetricsSink, Observer, TrembitaEvent,
    admin_tls_config,
};
use trembita_net::transport::RequestHandler;
use trembita_net::{
    BackoffPolicy, LocalNetwork, LocalTransport, PeerDirectory, QuicServer, QuicTransport,
    TrafficPolicy, Transport, TransportError, client_config, fetch_peers, send_join_request,
    server_config,
};
use trembita_proto::{
    CatalogCommand, JoinRejection, JoinRequest, JoinResponse, JoinRole, Membership, NodeId,
    PROTOCOL_VERSION, QueueAutoscalePolicyCommand,
};
use trembita_storage::GroupRedbLayout;

use trembita_actor_store::{
    ClusterActorStateStore, DEFAULT_ACTOR_STORE_GC_MAX_KEYS, DEFAULT_ACTOR_STORE_GC_PERIOD,
    RedbActorStateStore, StoreService, run_actor_store_gc_ticker,
};
use trembita_events::{
    ClusterEventTopic, EventOutboxCursor, EventOutboxDrainOpts, EventOutboxPoll, EventOutboxSource,
    EventTopic, InMemoryEventOutboxCursor, RedbEventOutboxCursor, RedbEventTopic,
    TopicRetentionOpts, TopicService, TopicSubscriptionDef, run_event_outbox_drainer,
};
use trembita_jobs::{
    AutoscalePolicy, BacklogFeedOpts, BacklogRegistry, BacklogSettleOutbox,
    BacklogSettleOutboxOpts, ClusterJobQueue, CompositeScheduleSource, DEFAULT_QUEUE_PREFETCH,
    ExternalBacklog, InMemoryBacklogSettleOutbox, JobQueue, MembershipAutoscalePolicy,
    QueueAutoscaleRegistry, QueueService, RecurringJob, RedbBacklogSettleOutbox, RedbJobQueue,
    SchedulePoll, ScheduleSource, ShardedJobQueue, StaticScheduleSource, WorkloadMetricsSnapshot,
    WorkloadOpts, run_backlog_feeder, run_backlog_settle_drainer, run_queue_autoscaler,
    run_queue_membership_autoscaler, run_queue_schedule_ticker, run_workload_governor,
};
use trembita_runtime::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterMessaging, ClusterState,
    ClusterSupervisor, ComputeTokenPool, DEFAULT_DRAIN_TIMEOUT, DirectoryPolicy, DirectoryRetry,
    DirectorySync, LeaderGate, LeaderLoopOpts, MailboxSpool, NodeService, RaftDriver,
    RedbMailboxSpool, ResourceProfile, RuntimeConfig, UserActor, VpsResources, run_leader_loop,
    run_mailbox_spool_drainer, spawn_multi_raft_node, spawn_node,
};

use crate::certs::{CertReloadHandle, PemSecurity, cert_paths_for_node};
use crate::cluster_handle::{ClusterFacts, TrembitaCluster};
use crate::discovery::Seed;
use crate::gateway::ConnectionTracker;
use crate::handler::{NoPeers, NodeRouter, PeerSource, QuicPeers};
use crate::multi_raft::{ArcGroupMigrate, GroupMigratePort, MultiRaftState};
use crate::node_id;
use crate::observer::TrembitaObserver;
use crate::security::Security;
use crate::workload::WorkloadRuntime;

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; actor counts fit in practice.
fn metric_usize(v: usize) -> f64 {
    v as f64
}

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; mailbox depths fit in practice.
fn metric_i64(v: i64) -> f64 {
    v as f64
}

#[allow(clippy::cast_precision_loss)] // Prometheus counters use f64; message counts fit in practice.
fn metric_u64(v: u64) -> f64 {
    v as f64
}

/// An error starting a node over the live QUIC transport
/// ([`start_quic`](TrembitaClusterBuilder::start_quic)).
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The mTLS server/client configuration could not be built.
    #[error("tls configuration: {0}")]
    Tls(#[from] trembita_net::TlsError),

    /// The QUIC listener could not bind `addr`.
    #[error("bind {addr}: {source}")]
    Bind {
        /// The address the listener tried to bind.
        addr: SocketAddr,
        /// The underlying transport error.
        source: TransportError,
    },

    /// A dynamic join via [`join`](TrembitaClusterBuilder::join) could not be
    /// completed (seed unreachable, no leader, or the cluster refused it).
    #[error("cluster join failed: {0}")]
    Join(String),

    /// Environment or app configuration could not be parsed.
    #[error("configuration: {0}")]
    Config(String),
}

/// Type-erased "register this actor type on the control plane" step.
type RegisterFn = Box<dyn FnOnce(&ClusterControl) + Send>;
/// Type-erased "declare this managed group on the supervisor" step.
type ManageFn = Box<dyn FnOnce(&ClusterSupervisor<Arc<ClusterFacts>>) + Send>;
/// Type-erased user leader task registered via [`TrembitaClusterBuilder::on_leader`].
type UserLeaderTask =
    Arc<dyn Fn(LeaderGate) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct UserLeaderTaskSpec {
    opts: LeaderLoopOpts,
    tick: UserLeaderTask,
}

/// Type-erased queue autoscale background task spawned at node start.
type AutoscaleTask = Box<
    dyn FnOnce(
            Arc<ClusterControl>,
            Arc<dyn ClusterState>,
            Arc<ActorDirectory>,
            HashMap<String, Arc<dyn JobQueue>>,
            Arc<QueueAutoscaleRegistry>,
            Arc<BacklogRegistry>,
        ) -> tokio::task::JoinHandle<()>
        + Send,
>;

#[derive(Debug, Clone)]
struct JobStreamSpec {
    name: String,
    path: Option<PathBuf>,
    lease_timeout: Duration,
    prefetch: usize,
    default_max_attempts: u32,
}

#[derive(Debug, Clone)]
struct TopicStreamSpec {
    name: String,
    path: Option<PathBuf>,
    lease_timeout: Duration,
    retention: TopicRetentionOpts,
    subscriptions: Vec<TopicSubscriptionDef>,
}

struct TopicLeaderLoop {
    bootstrapped: bool,
    service: Arc<TopicService>,
    specs: Vec<TopicStreamSpec>,
}

impl TopicLeaderLoop {
    async fn tick(&mut self, gate: LeaderGate) {
        if gate.first_in_term() && !self.bootstrapped {
            for spec in &self.specs {
                if !spec.subscriptions.is_empty() {
                    let _ = self
                        .service
                        .bootstrap_subscriptions(&spec.name, &spec.subscriptions)
                        .await;
                }
            }
            self.bootstrapped = true;
        }
        let _ = self.service.enforce_retention_all().await;
    }
}

#[derive(Debug, Clone)]
struct ShardedJobSpec {
    name: String,
    shard_count: usize,
}

#[derive(Debug, Clone)]
struct RecurringJobSpec {
    stream: String,
    job: RecurringJob,
}

/// Type-erased membership autoscale background task spawned at node start.
type MembershipAutoscaleTask = Box<
    dyn FnOnce(
            Arc<dyn ClusterState>,
            HashMap<String, Arc<dyn JobQueue>>,
            Arc<QueueAutoscaleRegistry>,
            Arc<BacklogRegistry>,
        ) + Send,
>;

#[derive(Clone)]
struct BacklogFeedSpec {
    stream: String,
    backlog: Arc<dyn ExternalBacklog>,
    opts: BacklogFeedOpts,
}

#[derive(Clone)]
struct ScheduleSourceSpec {
    stream: String,
    source: Arc<dyn ScheduleSource>,
    poll: Duration,
}

#[derive(Clone)]
struct EventOutboxFeedSpec {
    topic: String,
    source: Arc<dyn EventOutboxSource>,
    opts: EventOutboxDrainOpts,
}

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

/// Builder options set explicitly in code (not derived from env merge).
#[derive(Debug, Clone, Copy, Default)]
struct BuilderOverrides {
    node_id: bool,
    members: bool,
    allow_join: bool,
    allow_voter_join: bool,
    join_role: bool,
    allow_leave: bool,
    voter_replacement: bool,
    voter_replacement_grace_ticks: bool,
    drain_timeout: bool,
    cert_watch: bool,
}

impl<M: StateMachine + Default + 'static> TrembitaClusterBuilder<M> {
    /// Start a builder for node `node_id` running `machine`. Defaults to a
    /// single-node cluster (`members = [node_id]`); call [`members`](Self::members)
    /// for a multi-node bootstrap.
    #[must_use]
    pub fn new(node_id: NodeId, machine: M) -> Self {
        Self {
            node_id,
            machine,
            members: vec![node_id],
            raft: Config::default(),
            runtime: RuntimeConfig::default(),
            dev_multi_workers: false,
            resource_profile: ResourceProfile::default(),
            forward_timeout: Duration::from_secs(5),
            reconcile_period: Duration::from_millis(250),
            publish_period: Duration::from_millis(250),
            refresh_period: Duration::from_millis(50),
            event_capacity: 1024,
            metrics_sink: None,
            admin_addr: None,
            admin_tls: None,
            join_seeds: Vec::new(),
            join_role: JoinRole::Learner,
            traffic_policy: TrafficPolicy::unlimited(),
            raft_groups: 1,
            shard_count: 256,
            shard_routing: trembita_core::ShardRoutingKind::StableVirtual,
            group_replication_factor: DEFAULT_GROUP_REPLICATION_FACTOR,
            group_learner_factor: DEFAULT_GROUP_LEARNER_FACTOR,
            raft_machines: None,
            data_dir: None,
            actor_state_store: None,
            auto_durable_actor_store: true,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            directory_policy: DirectoryPolicy::default(),
            directory_retry: DirectoryRetry::default(),
            cert_watch: None,
            registrations: Vec::new(),
            managed: Vec::new(),
            job_streams: Vec::new(),
            topic_streams: Vec::new(),
            job_sharded: Vec::new(),
            recurring_jobs: Vec::new(),
            job_autoscale: Vec::new(),
            job_membership_autoscale: Vec::new(),
            queue_autoscale_meta: BTreeMap::new(),
            backlog_feeds: Vec::new(),
            schedule_sources: Vec::new(),
            event_outbox_feeds: Vec::new(),
            workload: None,
            durable_mailbox: false,
            leader_tasks: Vec::new(),
            overrides: BuilderOverrides::default(),
        }
    }

    /// Mark [`node_id`](Self::new) as code-authoritative for env merge (used by [`TrembitaConfigure`](crate::configure::TrembitaConfigure)).
    #[must_use]
    pub(crate) fn with_explicit_node_id(mut self) -> Self {
        self.overrides.node_id = true;
        self
    }

    /// Set the initial cluster membership (voting nodes) to bootstrap with.
    #[must_use]
    pub fn members(mut self, members: impl IntoIterator<Item = NodeId>) -> Self {
        self.members = members.into_iter().collect();
        if self.members.is_empty() {
            self.members.push(self.node_id);
        }
        self.overrides.members = true;
        self
    }

    /// Static voter bootstrap for the first `count` nodes (`NodeId(1)` … `NodeId(count)`).
    ///
    /// Sugar for multi-node seeds without listing ids manually; prefer [`members`](Self::members)
    /// or `TREMBITA_PEERS` when addresses differ per host.
    #[must_use]
    pub fn voters(mut self, count: u32) -> Self {
        assert!(count >= 1, "voters(count) requires count >= 1");
        self.members = (1..=count).map(|id| NodeId(u64::from(id))).collect();
        self.overrides.members = true;
        self
    }

    /// Override the core Raft timing configuration (election/heartbeat ticks).
    #[must_use]
    pub fn raft_config(mut self, config: Config) -> Self {
        self.raft = config;
        self
    }

    /// Host `count` independent Raft groups on this node (multi-Raft, write-sharding-multi-raft).
    /// Requires `M: Clone`; each group gets a clone of the builder's state
    /// machine. Defaults to `1` (single-group, unchanged behaviour).
    #[must_use]
    pub fn raft_groups(mut self, count: u32) -> Self {
        self.raft_groups = count.max(1);
        self
    }

    /// Fixed shard count for [`raft_groups`](Self::raft_groups) routing
    /// (defaults to 256).
    #[must_use]
    pub fn shard_count(mut self, count: u32) -> Self {
        self.shard_count = count.max(1);
        self
    }

    /// Use stable virtual shard routing (stable virtual default). Keys outside the active
    /// prefix are rejected; use [`TrembitaCluster::activate_shards`] to grow capacity
    /// without remapping existing keys.
    #[must_use]
    pub fn stable_shards(mut self, enabled: bool) -> Self {
        self.shard_routing = if enabled {
            trembita_core::ShardRoutingKind::StableVirtual
        } else {
            trembita_core::ShardRoutingKind::Modulus
        };
        self
    }

    /// modulus routing routing (`hash(key) % count`). Keys **remap** when the count
    /// grows — prefer the default stable virtual routing for new clusters.
    #[must_use]
    pub fn modulus_shards(mut self) -> Self {
        self.shard_routing = trembita_core::ShardRoutingKind::Modulus;
        self
    }

    /// Enable optional cross-shard two-phase commit on every Raft group runtime.
    #[must_use]
    pub fn cross_shard_2pc(mut self, enabled: bool) -> Self {
        self.runtime.cross_shard_2pc = enabled;
        self
    }

    /// Persist cross-shard 2PC prepare/abort in each group's Raft log so prepares
    /// survive leader restarts. Implies [`cross_shard_2pc`](Self::cross_shard_2pc).
    #[must_use]
    pub fn durable_cross_shard_2pc(mut self, enabled: bool) -> Self {
        if enabled {
            self.runtime.cross_shard_2pc = true;
            self.runtime.durable_cross_shard_2pc = true;
            if self.runtime.two_phase_prepare_timeout.is_none() {
                self.runtime.two_phase_prepare_timeout = Some(Duration::from_millis(
                    trembita_core::TWO_PHASE_DEFAULT_PREPARE_TIMEOUT_MS,
                ));
            }
        } else {
            self.runtime.durable_cross_shard_2pc = false;
        }
        self
    }

    /// Timeout after which a staged 2PC prepare is garbage-collected (leader-only).
    /// Applies to ephemeral and durable 2PC. Pass [`Duration::ZERO`] to disable.
    #[must_use]
    pub fn two_phase_prepare_timeout(mut self, timeout: Duration) -> Self {
        self.runtime.two_phase_prepare_timeout = if timeout.is_zero() {
            None
        } else {
            Some(timeout)
        };
        self
    }

    /// Replication factor for each shard Raft group's voter set (per-group-raft-membership).
    /// Clamped to the live node count at runtime; default 3. Use a value ≥
    /// expected cluster size to replicate on every joined node.
    #[must_use]
    pub fn group_replication_factor(mut self, factor: u32) -> Self {
        self.group_replication_factor = factor.max(1);
        self
    }

    /// Non-voting learner replicas per Raft group beyond
    /// [`group_replication_factor`](Self::group_replication_factor) voters plus optional learners.
    /// `0` disables learners (default).
    #[must_use]
    pub fn group_learner_factor(mut self, factor: u32) -> Self {
        self.group_learner_factor = factor;
        self
    }

    /// One state machine instance per Raft group. Required when
    /// [`raft_groups`](Self::raft_groups) is greater than 1; sets the group
    /// count from the slice length.
    ///
    /// # Panics
    /// If `machines` is empty.
    #[must_use]
    pub fn raft_machines(mut self, machines: impl IntoIterator<Item = M>) -> Self {
        let machines: Vec<M> = machines.into_iter().collect();
        assert!(
            !machines.is_empty(),
            "raft_machines requires at least one machine"
        );
        self.raft_groups = u32::try_from(machines.len()).expect("raft group count fits u32");
        self.raft_machines = Some(machines);
        self
    }

    /// Persist each Raft group's log/hard-state/snapshot under `path` as
    /// `group-<id>.redb` files (multi-Raft, write-sharding-multi-raft). Single-group nodes use
    /// `group-0.redb`.
    #[must_use]
    pub fn data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(path.into());
        self
    }

    /// Apply `TREMBITA_*` cluster settings when not already set in code.
    pub(crate) fn merge_app_config(mut self, cfg: &crate::env_config::AppConfig) -> Self {
        if !self.overrides.node_id {
            self.node_id = cfg.node_id;
        }
        if !self.overrides.members && cfg.env.peers {
            self.members = cfg.members.clone();
            if self.members.is_empty() {
                self.members.push(self.node_id);
            }
        }
        if self.data_dir.is_none()
            && let Some(dir) = cfg.data_dir.clone()
        {
            self.data_dir = Some(dir);
        }
        if !self.overrides.allow_join {
            self.runtime.allow_join = cfg.allow_join;
        }
        if !self.overrides.allow_voter_join {
            self.runtime.allow_voter_join = cfg.allow_voter_join;
        }
        if !self.overrides.allow_leave {
            self.runtime.allow_leave = cfg.allow_leave;
        }
        if !self.overrides.join_role {
            self.join_role = cfg.join_role;
        }
        if !self.overrides.voter_replacement {
            self.runtime.voter_replacement = cfg.voter_replacement;
        }
        if !self.overrides.voter_replacement_grace_ticks
            && let Some(ticks) = cfg.voter_replacement_grace_ticks
        {
            self.runtime.voter_replacement_grace_ticks = Some(ticks);
        }
        if !self.overrides.drain_timeout {
            self.drain_timeout = cfg.drain_timeout;
        }
        if !self.overrides.cert_watch {
            self.cert_watch = Some(cfg.cert_watch);
        }
        if !cfg.join_seeds.is_empty() {
            self.join_seeds = cfg.join_seeds.clone();
        }
        if self.admin_addr.is_none()
            && let Some(admin) = cfg.admin
        {
            self.admin_addr = Some(admin);
        }
        if self.admin_tls.is_none()
            && let Some((cert, key)) = cfg.admin_tls.clone()
        {
            self.admin_tls = Some(AdminTlsPaths { cert, key });
        }
        self
    }

    /// Write-ahead outbox/inbox for cross-node actor delivery (mailbox spool).
    /// Requires [`data_dir`](Self::data_dir); stores `{data_dir}/mailbox-spool.redb`.
    #[must_use]
    pub fn durable_mailbox(mut self, enabled: bool) -> Self {
        self.durable_mailbox = enabled;
        self
    }

    /// External store for **stateful actor workflow data** ([`RedbActorStateStore`](trembita_actor_store::RedbActorStateStore)
    /// when [`data_dir`](Self::data_dir) is set). Override with an explicit store when needed.
    #[must_use]
    pub fn actor_state_store(
        mut self,
        store: Arc<dyn trembita_actor_store::ActorStateStore>,
    ) -> Self {
        self.actor_state_store = Some(store);
        self
    }

    /// When `true` (default) and [`data_dir`](Self::data_dir) is set without an explicit
    /// [`actor_state_store`](Self::actor_state_store), open `{data_dir}/actor-store.redb`.
    #[must_use]
    pub fn auto_durable_actor_store(mut self, enabled: bool) -> Self {
        self.auto_durable_actor_store = enabled;
        self
    }

    /// Wall-clock duration of one logical Raft tick.
    #[must_use]
    pub fn tick_period(mut self, period: Duration) -> Self {
        self.runtime.tick_period = period;
        self
    }

    /// Override automatic Raft log compaction thresholds.
    ///
    /// Default is [`trembita_core::CompactionPolicy::default_auto`] (1024 entries or 4 MiB).
    #[must_use]
    pub fn auto_compaction(mut self, policy: trembita_core::CompactionPolicy) -> Self {
        self.runtime.compaction = policy;
        self
    }

    /// Disable automatic log compaction (use [`trembita_runtime::NodeHandle::compact`] manually).
    #[must_use]
    pub fn auto_compaction_disabled(mut self) -> Self {
        self.runtime.compaction = trembita_core::CompactionPolicy::disabled();
        self
    }

    /// Accept cluster joins on this node (`--allow-join`, join-rpc).
    #[must_use]
    pub fn allow_join(mut self, allow: bool) -> Self {
        self.runtime.allow_join = allow;
        self.overrides.allow_join = true;
        self
    }

    /// Accept [`JoinRole::Voter`] on `/cluster/join`. Default is learner-only
    /// elastic join; enable on the seed when joiners request voter role via
    /// [`join_as`](Self::join_as) or `TREMBITA_JOIN_ROLE=voter`.
    #[must_use]
    pub fn allow_voter_join(mut self, allow: bool) -> Self {
        self.runtime.allow_voter_join = allow;
        self.overrides.allow_voter_join = true;
        self
    }

    /// When `true` (default), the leader replaces a permanently unreachable
    /// voter by promoting the lowest-id caught-up learner.
    #[must_use]
    pub fn voter_replacement(mut self, enabled: bool) -> Self {
        self.runtime.voter_replacement = enabled;
        self.overrides.voter_replacement = true;
        self
    }

    /// Override the logical-tick grace period before an unreachable voter is
    /// replaced (tests / tuning). Default: `6 ×` reachability window.
    #[must_use]
    pub fn voter_replacement_grace_ticks(mut self, ticks: u64) -> Self {
        self.runtime.voter_replacement_grace_ticks = Some(ticks);
        self.overrides.voter_replacement_grace_ticks = true;
        self
    }

    /// Accept cluster leaves on this node (`--allow-leave`). When enabled, the
    /// leader commits a group-0 membership change removing the departing node
    /// from the request's `node_id` field.
    #[must_use]
    pub fn allow_leave(mut self, allow: bool) -> Self {
        self.runtime.allow_leave = allow;
        self.overrides.allow_leave = true;
        self
    }

    /// Join an **existing** cluster dynamically by contacting `seed` (a member's
    /// id + address) instead of pre-configuring every peer's address (discovery,
    /// join-rpc). On [`start_quic`](Self::start_quic) this node fetches the
    /// cluster's peer-address book from the seed, then sends a `/cluster/join`
    /// (the seed forwards to the leader), which commits a membership change
    /// adding this node. Peer addresses propagate both ways over `/cluster/peers`
    /// so every node — including this one — learns how to reach the others.
    ///
    /// Set [`members`](Self::members) to the cluster's *current* voter set (which
    /// does **not** include this new node). The committed role comes from
    /// [`join_as`](Self::join_as) (default [`JoinRole::Learner`]). `advertise_addr`
    /// defaults to the QUIC `listen` address passed to `start_quic`.
    #[must_use]
    pub fn join(mut self, seed: NodeId, addr: SocketAddr) -> Self {
        self.join_seeds = vec![Seed::new(seed, addr)];
        self
    }

    /// Join an existing cluster by bootstrapping against a **seed set** (discovery
    /// gossip discovery): an ordered list of candidate members. On
    /// [`start_quic`](Self::start_quic) this node tries each seed in turn — for
    /// pulling the peer-address book and for the join request — so a single dead
    /// or relocated seed does not block the join. Preferred seeds first;
    /// duplicates and any entry equal to this node are ignored.
    ///
    /// Supersedes [`join`](Self::join) (a single-seed convenience). Populate the
    /// list statically, or resolve it from orchestrated DNS with
    /// [`discovery::resolve_dns_seeds`](crate::discovery::resolve_dns_seeds).
    #[must_use]
    pub fn join_seeds(mut self, seeds: impl IntoIterator<Item = Seed>) -> Self {
        self.join_seeds = seeds.into_iter().collect();
        self
    }

    /// Role requested when this node dynamically joins via [`join`](Self::join) /
    /// [`join_seeds`](Self::join_seeds). Default [`JoinRole::Learner`] (elastic
    /// scale-out). Use [`JoinRole::Voter`] only when the seed has
    /// [`allow_voter_join`](Self::allow_voter_join). For a fixed voter set at
    /// bootstrap, prefer static [`members`](Self::members) / `TREMBITA_PEERS`.
    #[must_use]
    pub fn join_as(mut self, role: JoinRole) -> Self {
        self.join_role = role;
        self.overrides.join_role = true;
        self
    }

    /// Per-traffic-class QUIC admission control (future-work-and-risks R2). Rate-limit bulk
    /// client/actor traffic so latency-sensitive Raft consensus RPCs are never
    /// starved on the shared UDP socket. Defaults to
    /// [`TrafficPolicy::unlimited`]; consensus (`TrafficClass::Peer`) should
    /// be left unthrottled. Only affects the QUIC transport
    /// ([`start_quic`](Self::start_quic)).
    ///
    /// ```no_run
    /// use trembita::net::{TrafficClass, TrafficPolicy};
    /// let policy = TrafficPolicy::unlimited()
    ///     .with_rate(TrafficClass::Client, 5_000.0, 500.0)
    ///     .with_rate(TrafficClass::Actor, 20_000.0, 2_000.0);
    /// ```
    #[must_use]
    pub fn traffic_policy(mut self, policy: TrafficPolicy) -> Self {
        self.traffic_policy = policy;
        self
    }

    /// Permit multiple local instances per actor name (`--dev-multi-workers`,
    /// one-worker-per-vps). Off by default: production keeps one worker per node per name.
    #[must_use]
    pub fn dev_multi_workers(mut self, dev: bool) -> Self {
        self.dev_multi_workers = dev;
        self
    }

    /// How much of this VPS the single worker should use (one-worker-per-vps). Defaults to
    /// [`ResourceProfile::UseAllAvailable`]; retrieve the detected capacity from
    /// [`TrembitaCluster::vps_resources`] after start.
    #[must_use]
    pub fn resource_profile(mut self, profile: ResourceProfile) -> Self {
        self.resource_profile = profile;
        self
    }

    /// Deadline for proxying a client request to the leader (client-routing).
    #[must_use]
    pub fn forward_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout = timeout;
        self
    }

    /// How often the leader reconciles managed/auto-worker groups (supervisor-leader).
    #[must_use]
    pub fn reconcile_period(mut self, period: Duration) -> Self {
        self.reconcile_period = period;
        self
    }

    /// Register a leader-only periodic task ([leader-task](../../docs/decisions/leader-task.md)).
    ///
    /// The closure runs on each tick while this node holds Raft leadership.
    /// Use [`LeaderGate::first_in_term`] for one-shot work after election.
    #[must_use]
    pub fn on_leader<F, Fut>(mut self, opts: LeaderLoopOpts, f: F) -> Self
    where
        F: Fn(LeaderGate) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.leader_tasks.push(UserLeaderTaskSpec {
            opts,
            tick: Arc::new(move |gate| Box::pin(f(gate))),
        });
        self
    }

    /// How often this node republishes its local actor set to peers (E7
    /// anti-entropy).
    #[must_use]
    pub fn directory_publish_period(mut self, period: Duration) -> Self {
        self.publish_period = period;
        self
    }

    /// Graceful-drain timeout for actor stop/migration ([drain-timeout]).
    #[must_use]
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self.overrides.drain_timeout = true;
        self
    }

    /// Directory visibility for cross-node casts/asks. [`DirectoryPolicy::ReadYourWrites`]
    /// retries briefly when a target is not yet visible after spawn/scale.
    #[must_use]
    pub fn directory_policy(mut self, policy: DirectoryPolicy) -> Self {
        self.directory_policy = policy;
        self
    }

    /// Retry budget when [`directory_policy`](Self::directory_policy) is
    /// [`DirectoryPolicy::ReadYourWrites`].
    #[must_use]
    pub fn directory_retry(mut self, retry: DirectoryRetry) -> Self {
        self.directory_retry = retry;
        self
    }

    /// How often consensus status is mirrored into the supervisor and telemetry
    /// (membership + reachability deltas, liveness-vs-membership).
    #[must_use]
    pub fn refresh_period(mut self, period: Duration) -> Self {
        self.refresh_period = period;
        self
    }

    /// Capacity of the telemetry [`EventBus`] ring buffer per subscriber.
    #[must_use]
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity.max(1);
        self
    }

    /// Forward every runtime metrics sample to an external [`MetricsSink`]
    /// while keeping the admin `GET /metrics` Prometheus registry updated.
    #[must_use]
    pub fn metrics_sink(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics_sink = Some(sink);
        self
    }

    /// Serve the admin HTTP/1.1 endpoints (health, readiness, metrics,
    /// introspection, dashboard) on `addr` (default `0.0.0.0:8080`, health-admin-port).
    #[must_use]
    pub fn admin_addr(mut self, addr: SocketAddr) -> Self {
        self.admin_addr = Some(addr);
        self
    }

    /// Serve admin over **TLS** (server-only) using PEM `cert` and `key`
    /// (admin TLS). Plain HTTP is used when unset.
    #[must_use]
    pub fn admin_tls(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.admin_tls = Some(AdminTlsPaths {
            cert: cert.into(),
            key: key.into(),
        });
        self
    }

    /// Tune leader-side reachability detection (reachability tuning).
    #[must_use]
    pub fn reachability(mut self, config: ReachabilityConfig) -> Self {
        self.raft.reachability = config;
        self
    }

    /// Poll on-disk PEM files every `period` and hot-reload TLS when they change
    /// ([certificates](decisions/certificates.md)).
    /// Used with [`start_quic_pem`](Self::start_quic_pem); defaults to **60s** when unset.
    #[must_use]
    pub fn cert_watch(mut self, period: Duration) -> Self {
        self.cert_watch = Some(period);
        self.overrides.cert_watch = true;
        self
    }

    /// Register actor type `A` so this node can host it (locally, on remote
    /// spawn, or as a migration target). Managed groups register their type
    /// automatically; use this for types you spawn imperatively.
    #[must_use]
    pub fn register_actor<A: UserActor>(mut self) -> Self {
        self.registrations
            .push(Box::new(|control: &ClusterControl| {
                control.register_type::<A>();
            }));
        self
    }

    /// Keep exactly `total` instances of actor `A` (named `name`) placed across
    /// the cluster, reconciled by the leader (one-worker-per-vps).
    #[must_use]
    pub fn manage<A>(mut self, name: &str, total: usize, config: A::Config) -> Self
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        let name = name.to_string();
        self.managed.push(Box::new(
            move |sup: &ClusterSupervisor<Arc<ClusterFacts>>| {
                sup.manage::<A>(&name, total, config);
            },
        ));
        self
    }

    /// Enable a durable job queue stream at `{data_dir}/queue-{name}.redb`
    /// ([job-queue](../../docs/decisions/job-queue.md)). Requires [`data_dir`](Self::data_dir).
    #[must_use]
    pub fn job_queue(mut self, name: &str, lease_timeout: Duration) -> Self {
        self.job_streams.push(JobStreamSpec {
            name: name.to_string(),
            path: None,
            lease_timeout,
            prefetch: DEFAULT_QUEUE_PREFETCH,
            default_max_attempts: 0,
        });
        self
    }

    /// Like [`job_queue`](Self::job_queue) but opens an explicit redb path (tests, custom layout).
    #[must_use]
    pub fn job_queue_at(
        mut self,
        name: &str,
        path: impl Into<PathBuf>,
        lease_timeout: Duration,
    ) -> Self {
        self.job_streams.push(JobStreamSpec {
            name: name.to_string(),
            path: Some(path.into()),
            lease_timeout,
            prefetch: DEFAULT_QUEUE_PREFETCH,
            default_max_attempts: 0,
        });
        self
    }

    /// Tune leader prefetch depth for `stream` (default [`DEFAULT_QUEUE_PREFETCH`]).
    ///
    /// Prefetch keeps recently enqueued payloads in RAM on the queue leader so
    /// [`lease`](trembita_jobs::JobQueue::lease) skips re-reading from `redb`.
    /// Set `prefetch` to `0` to disable.
    #[must_use]
    pub fn job_queue_prefetch(mut self, stream: &str, prefetch: usize) -> Self {
        for spec in &mut self.job_streams {
            if spec.name == stream {
                spec.prefetch = prefetch;
            }
        }
        self
    }

    /// Default delivery-attempt ceiling for `stream` (`0` = unlimited retries).
    ///
    /// Applies to every enqueue that leaves
    /// [`EnqueueOptions::max_attempts`](trembita_jobs::EnqueueOptions::max_attempts)
    /// unset — including HTTP `POST /jobs/{stream}` and cron schedules. An
    /// explicit per-job ceiling always wins.
    #[must_use]
    pub fn job_queue_max_attempts(mut self, stream: &str, max_attempts: u32) -> Self {
        for spec in &mut self.job_streams {
            if spec.name == stream {
                spec.default_max_attempts = max_attempts;
            }
        }
        self
    }

    /// Enable a durable event topic at `{data_dir}/topic-{name}.redb`
    /// ([event-topics](../../docs/decisions/event-topics.md)). Requires [`data_dir`](Self::data_dir).
    #[must_use]
    pub fn event_topic(mut self, name: &str, lease_timeout: Duration) -> Self {
        self.topic_streams.push(TopicStreamSpec {
            name: name.to_string(),
            path: None,
            lease_timeout,
            retention: TopicRetentionOpts::default(),
            subscriptions: Vec::new(),
        });
        self
    }

    /// Like [`event_topic`](Self::event_topic) but opens an explicit redb path.
    #[must_use]
    pub fn event_topic_at(
        mut self,
        name: &str,
        path: impl Into<PathBuf>,
        lease_timeout: Duration,
    ) -> Self {
        self.topic_streams.push(TopicStreamSpec {
            name: name.to_string(),
            path: Some(path.into()),
            lease_timeout,
            retention: TopicRetentionOpts::default(),
            subscriptions: Vec::new(),
        });
        self
    }

    /// Declare subscriptions and retention for a registered topic.
    #[must_use]
    pub fn event_topic_subscriptions(
        mut self,
        name: &str,
        subscriptions: &[TopicSubscriptionDef],
    ) -> Self {
        for spec in &mut self.topic_streams {
            if spec.name == name {
                spec.subscriptions = subscriptions.to_vec();
            }
        }
        self
    }

    /// Retention thresholds for a registered topic.
    #[must_use]
    pub fn event_topic_retention(mut self, name: &str, retention: TopicRetentionOpts) -> Self {
        for spec in &mut self.topic_streams {
            if spec.name == name {
                spec.retention = retention;
            }
        }
        self
    }

    /// Leader-fed stream backed by an [`ExternalBacklog`] ([external-backlog](../../docs/decisions/external-backlog.md)).
    ///
    /// Requires [`job_queue`](Self::job_queue) on the same `stream`. The leader claims from
    /// `backlog`, enqueues into the job queue with `dedup_key = item.key`, and calls
    /// [`ExternalBacklog::settle`] on terminal ack/dead-letter outcomes.
    #[must_use]
    pub fn job_queue_external_backlog(
        mut self,
        stream: &str,
        backlog: Arc<dyn ExternalBacklog>,
        opts: BacklogFeedOpts,
    ) -> Self {
        self.backlog_feeds.push(BacklogFeedSpec {
            stream: stream.to_string(),
            backlog,
            opts,
        });
        self
    }

    /// Per-node workload governor — compute tokens arbitrate gateway vs job handlers
    /// ([workload-governor](../../docs/decisions/workload-governor.md)).
    #[must_use]
    pub fn workload(mut self, opts: WorkloadOpts) -> Self {
        self.workload = Some(opts);
        self
    }

    /// Register a dynamic [`ScheduleSource`] for recurring jobs on `stream`
    /// ([schedule-source](../../docs/decisions/schedule-source.md)).
    ///
    /// Requires [`job_queue`](Self::job_queue) on the same stream. Pairs with
    /// [`.cron()`](crate::TrembitaAppBuilder::cron) — static and external sources
    /// are merged.
    #[must_use]
    pub fn schedule_source(
        mut self,
        stream: &str,
        source: Arc<dyn ScheduleSource>,
        poll: SchedulePoll,
    ) -> Self {
        self.schedule_sources.push(ScheduleSourceSpec {
            stream: stream.to_string(),
            source,
            poll: poll.duration(),
        });
        self
    }

    /// Register a transactional outbox drainer for `topic`
    /// ([event-outbox](../../docs/decisions/event-outbox.md)).
    ///
    /// Requires [`event_topic`](Self::event_topic) on the same name. The leader polls
    /// [`EventOutboxSource::poll`], publishes to the topic, then
    /// [`EventOutboxSource::mark_published`].
    #[must_use]
    pub fn event_outbox_source(
        mut self,
        topic: &str,
        source: Arc<dyn EventOutboxSource>,
        poll: EventOutboxPoll,
    ) -> Self {
        self.event_outbox_feeds.push(EventOutboxFeedSpec {
            topic: topic.to_string(),
            source,
            opts: EventOutboxDrainOpts::default().poll(poll.duration()),
        });
        self
    }

    /// Like [`event_outbox_source`](Self::event_outbox_source) with explicit drain tunables.
    #[must_use]
    pub fn event_outbox_source_with_opts(
        mut self,
        topic: &str,
        source: Arc<dyn EventOutboxSource>,
        opts: EventOutboxDrainOpts,
    ) -> Self {
        self.event_outbox_feeds.push(EventOutboxFeedSpec {
            topic: topic.to_string(),
            source,
            opts,
        });
        self
    }

    /// Register a cron-driven recurring job on `stream` ([`RecurringJob`]).
    ///
    /// Requires [`job_queue`](Self::job_queue) on the same stream. Schedules persist in
    /// `queue-{stream}.redb` and fire on the queue leader.
    #[must_use]
    pub fn recurring_job(mut self, stream: &str, job: RecurringJob) -> Self {
        self.recurring_jobs.push(RecurringJobSpec {
            stream: stream.to_string(),
            job,
        });
        self
    }

    /// Leader-only autoscale loop for `stream` depth → `policy.worker_group` count.
    /// Registers `A` on the control plane; pair with [`manage`](Self::manage) or
    /// [`manage_auto`](Self::manage_auto) for the same group name.
    ///
    /// # Panics
    /// If `stream` was not registered via [`job_queue`](Self::job_queue).
    #[must_use]
    pub fn job_queue_autoscale<A: UserActor>(
        mut self,
        stream: &str,
        policy: &AutoscalePolicy,
        config: A::Config,
    ) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        let stream = stream.to_string();
        let worker_group = policy.worker_group.clone();
        let policy = policy.clone();
        upsert_queue_autoscale_meta(
            &mut self.queue_autoscale_meta,
            &stream,
            Some(policy.to_wire()),
            None,
        );
        self.registrations
            .push(Box::new(|control: &ClusterControl| {
                control.register_type::<A>();
            }));
        self.job_autoscale.push(Box::new(
            move |control, state, directory, queues, registry, backlog_registry| {
                let Some(queue) = queues.get(&stream).cloned() else {
                    panic!(
                        "job_queue_autoscale stream {stream:?} was not registered via job_queue"
                    );
                };
                let backlog = backlog_registry.get(&stream);
                let policy = policy.clone();
                let config = config.clone();
                let worker_group = worker_group.clone();
                let stream = stream.clone();
                tokio::spawn(async move {
                    run_queue_autoscaler(
                        queue,
                        directory,
                        Arc::clone(&state),
                        registry,
                        stream,
                        policy,
                        backlog,
                        move |desired| {
                            let control = Arc::clone(&control);
                            let state = Arc::clone(&state);
                            let config = config.clone();
                            let worker_group = worker_group.clone();
                            async move {
                                control
                                    .scale_cluster::<A>(
                                        &worker_group,
                                        desired,
                                        config,
                                        &state.reachable_nodes(),
                                    )
                                    .await
                                    .map(|_| ())
                            }
                        },
                    )
                    .await;
                })
            },
        ));
        self
    }

    /// Federated queue over `shard_count` independent redb streams (`{name}~0` …)
    /// to spread leader replication load ([job-queue](../../docs/decisions/job-queue.md)).
    ///
    /// # Panics
    /// If `shard_count` is zero.
    #[must_use]
    pub fn job_queue_sharded(
        mut self,
        name: &str,
        shard_count: usize,
        lease_timeout: Duration,
    ) -> Self {
        assert!(
            shard_count >= 1,
            "job_queue_sharded requires shard_count >= 1"
        );
        for i in 0..shard_count {
            self.job_streams.push(JobStreamSpec {
                name: format!("{name}~{i}"),
                path: None,
                lease_timeout,
                prefetch: DEFAULT_QUEUE_PREFETCH,
                default_max_attempts: 0,
            });
        }
        self.job_sharded.push(ShardedJobSpec {
            name: name.to_string(),
            shard_count,
        });
        self
    }

    /// Leader-only loop: when queue depth per live node exceeds a threshold, call
    /// `join` to add a VPS (production scale-out beyond worker autoscale).
    ///
    /// # Panics
    /// If `stream` was not registered via [`job_queue`](Self::job_queue) or
    /// [`job_queue_sharded`](Self::job_queue_sharded).
    #[must_use]
    pub fn job_queue_membership_autoscale(
        mut self,
        stream: &str,
        policy: &MembershipAutoscalePolicy,
        join: impl Fn() -> trembita_actor_store::BoxFuture<
            'static,
            Result<(), trembita_runtime::ClusterScaleError>,
        > + Send
        + Sync
        + 'static,
    ) -> Self {
        let stream = stream.to_string();
        let policy = policy.clone();
        let join = Arc::new(join);
        upsert_queue_autoscale_meta(
            &mut self.queue_autoscale_meta,
            &stream,
            None,
            Some(policy.to_wire()),
        );
        self.job_membership_autoscale
            .push(Box::new(move |state, queues, registry, backlog_registry| {
            let Some(queue) = queues.get(&stream).cloned() else {
                panic!(
                    "job_queue_membership_autoscale stream {stream:?} was not registered via job_queue or job_queue_sharded"
                );
            };
            let backlog = backlog_registry.get(&stream);
            let policy = policy.clone();
            let join = Arc::clone(&join);
            let stream = stream.clone();
            tokio::spawn(async move {
                run_queue_membership_autoscaler(
                    queue,
                    state,
                    registry,
                    stream,
                    policy,
                    backlog,
                    move || {
                        let join = Arc::clone(&join);
                        async move { join().await }
                    },
                )
                .await;
            });
        }));
        self
    }

    /// Declare an auto-worker group: one instance of `A` on every live node,
    /// tracking membership so new nodes get a worker automatically (auto-spawn-on-join).
    #[must_use]
    pub fn manage_auto<A>(mut self, name: &str, config: A::Config) -> Self
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        let name = name.to_string();
        self.managed.push(Box::new(
            move |sup: &ClusterSupervisor<Arc<ClusterFacts>>| {
                sup.manage_auto::<A>(&name, config);
            },
        ));
        self
    }

    /// Start the node over an in-memory [`LocalNetwork`] (tests, the simulator,
    /// and single-process multi-node dev clusters). Attaches this node's router
    /// to `net` under its id.
    ///
    /// Must run inside a Tokio runtime.
    pub async fn start_local(self, net: &LocalNetwork) -> TrembitaCluster<M> {
        let node_id = self.node_id;
        let transport: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), node_id));
        let peers: Arc<dyn PeerSource> = Arc::new(NoPeers);
        let (cluster, router) = self.assemble(transport, peers, None).await;
        net.attach(node_id, router);
        cluster
    }

    /// Start the node over the live HTTP/3-over-QUIC transport with mTLS (security,
    /// wire-transport) — the production path. Binds a QUIC listener on `listen`, dials
    /// peers found in `peers` (a [`NodeId`] → address book), and authenticates
    /// every connection with `security`.
    ///
    /// For a **static** cluster the `peers` directory should contain the address
    /// of every member (this node's own entry is ignored); give each node the
    /// same [`members`](Self::members) set and `peers` map. For **elastic**
    /// growth, pair [`join`](Self::join) with a `peers` map holding just the seed
    /// — addresses of the rest are discovered over `/cluster/peers` (discovery).
    ///
    /// Must run inside a Tokio runtime.
    ///
    /// # Errors
    /// Returns [`StartError`] if the mTLS configuration cannot be built, the
    /// QUIC listener cannot bind `listen`, or a requested dynamic
    /// [`join`](Self::join) could not be completed.
    pub async fn start_quic(
        self,
        security: Security,
        listen: SocketAddr,
        peers: PeerDirectory,
    ) -> Result<TrembitaCluster<M>, StartError> {
        self.start_quic_inner(security, listen, peers, None, None)
            .await
    }

    /// Like [`start_quic`](Self::start_quic) with PEM reload paths and optional cert directory.
    ///
    /// # Errors
    /// Same as [`start_quic`](Self::start_quic).
    pub async fn start_quic_cluster(
        self,
        security: Security,
        listen: SocketAddr,
        peers: PeerDirectory,
        pem_paths: Option<trembita_net::CertPaths>,
        cert_dir: Option<PathBuf>,
    ) -> Result<TrembitaCluster<M>, StartError> {
        self.start_quic_inner(security, listen, peers, pem_paths, cert_dir)
            .await
    }

    /// Like [`start_quic`](Self::start_quic) but loads from [`PemSecurity`] and,
    /// when [`cert_watch`](Self::cert_watch) is set (or by default every **60s**),
    /// hot-reloads TLS when the PEM files change. Also reloads on `SIGHUP` (Unix).
    ///
    /// # Errors
    /// Same as [`start_quic`](Self::start_quic).
    pub async fn start_quic_pem(
        self,
        pem: PemSecurity,
        listen: SocketAddr,
        peers: PeerDirectory,
    ) -> Result<TrembitaCluster<M>, StartError> {
        let paths = pem.paths.clone();
        self.start_quic_inner(pem.security, listen, peers, Some(paths), None)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn start_quic_inner(
        mut self,
        mut security: Security,
        listen: SocketAddr,
        mut peers: PeerDirectory,
        mut pem_paths: Option<trembita_net::CertPaths>,
        cert_dir: Option<PathBuf>,
    ) -> Result<TrembitaCluster<M>, StartError> {
        let dynamic_join = !self.join_seeds.is_empty();
        if let Some(ref data_dir) = self.data_dir {
            if let Some(persisted) = node_id::read_persisted(data_dir) {
                self.node_id = persisted;
                if !dynamic_join {
                    if !self.members.contains(&persisted) {
                        self.members.push(persisted);
                        self.members.sort();
                    }
                    if !peers.contains(persisted) {
                        peers.insert(persisted, listen);
                    }
                }
            } else if !dynamic_join {
                if self.node_id == NodeId(0) {
                    self.node_id = NodeId(1);
                }
                node_id::persist(data_dir, self.node_id)
                    .map_err(|e| StartError::Config(format!("persist node id: {e}")))?;
                if !self.members.contains(&self.node_id) {
                    self.members.push(self.node_id);
                    self.members.sort();
                }
                if !peers.contains(self.node_id) {
                    peers.insert(self.node_id, listen);
                }
            }
        }

        let mut server_cfg = server_config(&security.identity, security.roots.clone())?;
        let mut client_cfg = client_config(&security.identity, security.roots.clone())?;

        let server = Arc::new(
            QuicServer::bind(listen, server_cfg.clone()).map_err(|source| StartError::Bind {
                addr: listen,
                source,
            })?,
        );
        // Share the bound endpoint so outbound dials reuse the listener socket.
        let endpoint = server.endpoint().clone();
        let quic = Arc::new(QuicTransport::with_policy(
            endpoint,
            client_cfg.clone(),
            peers.clone(),
            BackoffPolicy::default(),
            self.traffic_policy.clone(),
        ));
        let seeds = crate::discovery::dedupe_seeds(self.join_seeds.iter().copied(), self.node_id);
        for seed in &seeds {
            quic.learn_peer(seed.node_id, seed.addr);
        }

        let mut pre_joined = false;
        if dynamic_join
            && self
                .data_dir
                .as_ref()
                .is_none_or(|dir| node_id::read_persisted(dir).is_none())
        {
            let (assigned, membership) =
                join_cluster_auto(&quic, &seeds, listen, self.join_role).await?;
            self.node_id = assigned;
            self.members = membership.voters.clone();
            pre_joined = true;
            if let Some(ref data_dir) = self.data_dir {
                node_id::persist(data_dir, assigned)
                    .map_err(|e| StartError::Config(format!("persist assigned node id: {e}")))?;
            }
            quic.learn_peer(assigned, listen);
            if !peers.contains(assigned) {
                peers.insert(assigned, listen);
            }
            if let Some(ref dir) = cert_dir {
                pem_paths = Some(cert_paths_for_node(dir, assigned));
                let loaded = PemSecurity::load(assigned, pem_paths.clone().unwrap())?;
                security = loaded.security;
                server_cfg = server_config(&security.identity, security.roots.clone())?;
                client_cfg = client_config(&security.identity, security.roots.clone())?;
                server.reload(server_cfg);
                quic.reload(client_cfg).await;
            }
        }

        let node_id = self.node_id;
        let join_role = self.join_role;
        quic.learn_peer(node_id, listen);
        let transport: Arc<dyn Transport> = quic.clone();
        let peer_source: Arc<dyn PeerSource> = Arc::new(QuicPeers(Arc::clone(&quic)));
        let sync_period = self.publish_period;
        let cert_watch = self.cert_watch;
        let (mut cluster, router) = self
            .assemble(transport, peer_source, Some(sync_period))
            .await;

        let accept = tokio::spawn({
            let server = Arc::clone(&server);
            async move { server.run_arc(router).await }
        });
        cluster.tasks.lock().unwrap().push(accept);

        // Dynamically join an existing cluster: learn peer addresses from a
        // reachable seed, then ask to join (the seed forwards to the leader).
        // Blocks until the membership change commits or a deadline elapses
        // (discovery, join-rpc); tries every seed for resilience.
        if !seeds.is_empty() && !pre_joined {
            join_cluster(&quic, node_id, &seeds, listen, join_role).await?;
        } else if pre_joined {
            // Confirm membership (Duplicate) after auto-assigned pre-join.
            let _ = join_cluster(&quic, node_id, &seeds, listen, join_role).await;
        }

        if let Some(paths) = pem_paths {
            let reload = Arc::new(CertReloadHandle::new(
                node_id,
                paths,
                server,
                quic,
                Arc::clone(&cluster.facts),
            ));
            let period = cert_watch.unwrap_or(Duration::from_secs(60));
            cluster
                .tasks
                .lock()
                .unwrap()
                .push(reload.clone().spawn_watcher(period));
            if let Some(sighup) = reload.clone().spawn_sighup() {
                cluster.tasks.lock().unwrap().push(sighup);
            }
            cluster.cert_reload = Some(reload);
        }

        Ok(cluster)
    }

    /// Assemble every runtime component over `transport`, spawn the background
    /// loops, and return the cluster handle plus the router to attach.
    #[allow(clippy::too_many_lines)] // single bootstrap path wiring transport, raft, actors, and background loops.
    async fn assemble(
        mut self,
        transport: Arc<dyn Transport>,
        peers: Arc<dyn PeerSource>,
        peer_sync: Option<Duration>,
    ) -> (TrembitaCluster<M>, Arc<dyn RequestHandler>) {
        let node_id = self.node_id;

        let vps_resources = VpsResources::detect(self.resource_profile);
        let resource_profile = self.resource_profile;

        // When joining dynamically, bootstrap consensus without this node in the
        // voter set — group 0 join + per-group sync add it later (per-group-raft-membership).
        let dynamic_join = !self.join_seeds.is_empty();
        let bootstrap_voters = consensus_bootstrap_voters(&self.members, node_id, dynamic_join);

        let metrics = match self.metrics_sink {
            Some(sink) => Metrics::with_extra_sinks(vec![sink]),
            None => Metrics::new(),
        };
        let on_two_phase_gc_aborted: trembita_runtime::TwoPhaseGcAbortedFn = Arc::new({
            let metrics = metrics.clone();
            move || crate::two_phase::record_two_phase_gc_aborted(&metrics, node_id.0)
        });
        self.runtime.on_two_phase_gc_aborted = Some(Arc::clone(&on_two_phase_gc_aborted));

        let saga_registry = Arc::new(Mutex::new(BTreeMap::new()));
        let saga_hook_reg = Arc::clone(&saga_registry);
        let on_saga_journal_applied: trembita_runtime::SagaJournalAppliedFn =
            Arc::new(move |cmd| {
                if let Ok(record) = trembita_client::decode_journal_record(&cmd.record) {
                    saga_hook_reg
                        .lock()
                        .expect("lock")
                        .insert(cmd.saga_id, record);
                }
            });

        let queue_autoscale_registry = Arc::new(QueueAutoscaleRegistry::new());
        let queue_autoscale_hook_reg = Arc::clone(&queue_autoscale_registry);
        let on_queue_autoscale_policy_applied: trembita_runtime::QueueAutoscalePolicyAppliedFn =
            Arc::new(move |cmd| {
                queue_autoscale_hook_reg.apply(&cmd);
            });

        let two_phase_registry = Arc::new(Mutex::new(BTreeMap::new()));
        let two_phase_hook_reg = Arc::clone(&two_phase_registry);
        let on_two_phase_journal_applied: trembita_runtime::TwoPhaseJournalAppliedFn =
            Arc::new(move |cmd| {
                if let Ok(record) = trembita_client::decode_two_phase_journal_record(&cmd.record) {
                    two_phase_hook_reg
                        .lock()
                        .expect("lock")
                        .insert(cmd.tx_id, record);
                }
            });

        // --- Consensus runtime -------------------------------------------
        let mut multi_raft = None;
        let mut catalog_event_rx = None;
        let mut meta_handle = None;
        let (handle, group_handles, consensus_service) = if self.raft_groups > 1 {
            let machines = self.raft_machines.unwrap_or_else(|| {
                panic!(
                    "raft_groups = {} requires .raft_machines(...) on the builder",
                    self.raft_groups
                )
            });
            assert_eq!(
                machines.len(),
                self.raft_groups as usize,
                "raft_machines length must match raft_groups"
            );
            let initial_catalog: Vec<trembita_core::RaftGroupId> = (0..self.raft_groups)
                .map(trembita_core::RaftGroupId)
                .collect();
            let catalog = Arc::new(Mutex::new(initial_catalog.clone()));
            let (catalog_tx, catalog_rx) = tokio::sync::mpsc::unbounded_channel::<CatalogCommand>();
            catalog_event_rx = Some(catalog_rx);
            let catalog_snap = Arc::clone(&catalog);
            let mut runtime_meta = self.runtime.clone();
            runtime_meta.catalog_snapshot =
                Some(Arc::new(move || catalog_snap.lock().unwrap().clone()));
            runtime_meta.on_catalog_applied = Some(Arc::new(move |cmd| {
                let _ = catalog_tx.send(cmd);
            }));
            runtime_meta.on_saga_journal_applied = Some(Arc::clone(&on_saga_journal_applied));
            runtime_meta.on_two_phase_journal_applied =
                Some(Arc::clone(&on_two_phase_journal_applied));
            runtime_meta.on_queue_autoscale_policy_applied =
                Some(Arc::clone(&on_queue_autoscale_policy_applied));
            let spawn = spawn_multi_raft_node(
                node_id,
                &bootstrap_voters,
                self.group_replication_factor,
                self.raft.clone(),
                &self.runtime,
                &runtime_meta,
                self.shard_count,
                self.shard_routing,
                self.raft_groups,
                machines,
                Arc::clone(&transport),
                self.forward_timeout,
                self.data_dir.as_deref(),
            )
            .expect("open multi-group raft storage");
            let sharded = spawn.sharded;
            meta_handle = Some(spawn.meta_handle);
            let mut handle_map = BTreeMap::new();
            for (i, h) in spawn.user_handles.into_iter().enumerate() {
                handle_map.insert(u32::try_from(i).expect("group index fits u32"), h);
            }
            let primary = handle_map.get(&0).cloned().expect("group 0 handle");
            let group_handles: Vec<_> = initial_catalog
                .iter()
                .filter_map(|g| handle_map.get(&g.0).cloned())
                .collect();
            multi_raft = Some(Arc::new(MultiRaftState {
                sharded: Arc::clone(&sharded),
                handles: Mutex::new(handle_map),
                transport: Arc::clone(&transport),
                raft: self.raft.clone(),
                runtime: self.runtime.clone(),
                forward_timeout: self.forward_timeout,
                data_dir: self.data_dir.clone(),
                catalog,
                node_id,
                replication_factor: self.group_replication_factor,
                learner_factor: self.group_learner_factor,
            }));
            (primary, group_handles, sharded as Arc<dyn RequestHandler>)
        } else {
            let driver = if let Some(ref dir) = self.data_dir {
                let storage = GroupRedbLayout::new(dir)
                    .open_group(0)
                    .expect("open raft storage");
                RaftDriver::recover(
                    node_id,
                    bootstrap_voters.iter().copied(),
                    self.raft.clone(),
                    self.machine,
                    Box::new(storage),
                )
                .expect("recover raft storage")
            } else {
                let node =
                    RaftNode::new(node_id, bootstrap_voters.iter().copied(), self.raft.clone());
                RaftDriver::new(node, self.machine)
            };
            let mut runtime = self.runtime.clone();
            runtime.on_saga_journal_applied = Some(Arc::clone(&on_saga_journal_applied));
            runtime.on_two_phase_journal_applied = Some(Arc::clone(&on_two_phase_journal_applied));
            runtime.on_queue_autoscale_policy_applied =
                Some(Arc::clone(&on_queue_autoscale_policy_applied));
            let handle = spawn_node(driver, Arc::clone(&transport), &runtime);
            let service = Arc::new(
                NodeService::new(handle.clone(), Arc::clone(&transport))
                    .with_forward_timeout(self.forward_timeout),
            ) as Arc<dyn RequestHandler>;
            (handle.clone(), vec![handle], service)
        };

        // --- Actor planes -------------------------------------------------
        let workload_opts = self.workload.clone();
        let compute_pool = workload_opts
            .as_ref()
            .map(|opts| ComputeTokenPool::new(opts.max_compute_tokens));
        let registry = if self.dev_multi_workers {
            ActorRegistry::new_dev()
        } else {
            ActorRegistry::new()
        };
        if let Some(pool) = compute_pool.as_ref() {
            registry.set_compute_tokens(Arc::clone(pool));
        }
        let directory = ActorDirectory::new();
        // Live leadership/membership facts, updated by the facts-refresher loop.
        // Created before the control plane so forwarded scales can be
        // leader-gated against it (supervisor-leader).
        let facts = Arc::new(ClusterFacts::default());
        let facts_state: Arc<dyn ClusterState> = facts.clone();
        let control = Arc::new(
            ClusterControl::new(
                node_id,
                registry.clone(),
                Arc::clone(&directory),
                Arc::clone(&transport),
            )
            .with_cluster_state(facts_state),
        );
        let messaging = Arc::new({
            let mut messaging = ClusterMessaging::with_policy(
                node_id,
                Arc::clone(&directory),
                registry.clone(),
                Arc::clone(&transport),
                self.directory_policy,
                self.directory_retry,
            );
            if self.durable_mailbox {
                let data_dir = self.data_dir.as_ref().unwrap_or_else(|| {
                    panic!("durable_mailbox requires data_dir on the cluster builder")
                });
                let spool = Arc::new(
                    RedbMailboxSpool::open(data_dir.join("mailbox-spool.redb"))
                        .expect("open mailbox spool redb"),
                ) as Arc<dyn MailboxSpool>;
                messaging = messaging.with_mailbox_spool(spool);
            }
            if let Some(pool) = compute_pool.as_ref() {
                messaging = messaging.with_compute_tokens(Arc::clone(pool));
            }
            messaging
        });
        if messaging.has_mailbox_spool() {
            messaging.drain_mailbox_spool_once().await;
        }
        let directory_sync = Arc::new(DirectorySync::new(
            node_id,
            Arc::clone(&directory),
            Arc::clone(&transport),
        ));

        // Register imperative actor types.
        for register in self.registrations {
            register(&control);
        }

        // --- Supervisor ---------------------------------------------------
        let supervisor = Arc::new(ClusterSupervisor::new(
            Arc::clone(&control),
            Arc::clone(&facts),
        ));
        for manage in self.managed {
            manage(&supervisor);
        }

        // --- Observability ------------------------------------------------
        let events = EventBus::new(self.event_capacity);
        // Surface actor lifecycle / restarts / escalations and (opt-in)
        // per-message traces as metrics + events (E14 → Track H, observability): the
        // registry stays telemetry-agnostic. Installed *before* any spawn so
        // every instance task binds the observer at launch.
        let telemetry = Arc::new(crate::cluster_handle::ActorTelemetry::new(
            node_id,
            events.clone(),
            metrics.clone(),
        ));
        registry.set_observer(telemetry.clone());

        // --- Job queue (leader wire service) ------------------------------
        let backlog_registry = Arc::new(BacklogRegistry::new());
        for feed in &self.backlog_feeds {
            backlog_registry.register(&feed.stream, Arc::clone(&feed.backlog));
        }
        let backlog_settle_outbox: Option<Arc<dyn BacklogSettleOutbox>> =
            if self.backlog_feeds.is_empty() {
                None
            } else {
                let outbox: Arc<dyn BacklogSettleOutbox> = if let Some(data_dir) =
                    self.data_dir.as_ref()
                {
                    Arc::new(
                        RedbBacklogSettleOutbox::open(data_dir.join("backlog-settle-outbox.redb"))
                            .unwrap_or_else(|e| {
                                panic!(
                                    "open backlog settle outbox at {}: {e}",
                                    data_dir.join("backlog-settle-outbox.redb").display()
                                )
                            }),
                    )
                } else {
                    Arc::new(InMemoryBacklogSettleOutbox::new())
                };
                Some(outbox)
            };
        let queue_service: Option<Arc<QueueService>> = if self.job_streams.is_empty() {
            None
        } else {
            let events_for_queue = events.clone();
            let metrics_for_queue = metrics.clone();
            let mut service = QueueService::new(
                node_id,
                Arc::clone(&facts) as Arc<dyn ClusterState>,
                Arc::clone(&transport),
            )
            .with_lifecycle_hook(Arc::new(move |ev| {
                // Attempts are recorded here, once per delivery — a metrics
                // sampler polling queue depth cannot see individual deliveries.
                if let trembita_jobs::QueueLifecycleEvent::Leased {
                    ref stream,
                    attempts,
                    ..
                } = ev
                {
                    metrics_for_queue.observe(
                        "trembita_queue_job_attempts",
                        "Delivery attempts per leased job (1 = first delivery).",
                        &[("stream", stream)],
                        f64::from(attempts),
                    );
                    if attempts > 1 {
                        metrics_for_queue.incr(
                            "trembita_queue_redeliveries_total",
                            "Job deliveries that were not the first attempt.",
                            &[("stream", stream)],
                            1.0,
                        );
                    }
                }
                let _ = events_for_queue.emit(TrembitaEvent::from_queue_lifecycle(ev));
            }));
            if let Some(outbox) = backlog_settle_outbox.as_ref() {
                service = service.with_backlog_settle_outbox(Arc::clone(outbox));
            }
            Some(Arc::new(service))
        };
        let mut job_queues: HashMap<String, Arc<dyn JobQueue>> = HashMap::new();
        let mut local_backends: HashMap<String, Arc<dyn JobQueue>> = HashMap::new();
        for spec in &self.job_streams {
            let path = match &spec.path {
                Some(path) => path.clone(),
                None => self
                    .data_dir
                    .as_ref()
                    .unwrap_or_else(|| {
                        panic!(
                            "job_queue({:?}) requires data_dir or job_queue_at with an explicit path",
                            spec.name
                        )
                    })
                    .join(format!("queue-{}.redb", spec.name)),
            };
            let local = Arc::new(
                RedbJobQueue::open(&path, spec.lease_timeout)
                    .unwrap_or_else(|e| panic!("open job queue at {}: {e}", path.display()))
                    .default_max_attempts(spec.default_max_attempts),
            );
            local_backends.insert(spec.name.clone(), Arc::clone(&local) as Arc<dyn JobQueue>);
            if let Some(service) = queue_service.as_ref() {
                service.register_redb_stream(&spec.name, &local, spec.prefetch);
            }
            let client: Arc<dyn JobQueue> = Arc::new(
                ClusterJobQueue::new(
                    &spec.name,
                    node_id,
                    Arc::clone(&facts) as Arc<dyn ClusterState>,
                    Arc::clone(&transport),
                )
                .default_max_attempts(spec.default_max_attempts),
            );
            job_queues.insert(spec.name.clone(), client);
        }

        for spec in &self.job_sharded {
            let shards: Vec<Arc<dyn JobQueue>> = (0..spec.shard_count)
                .map(|i| {
                    local_backends
                        .get(&format!("{}~{i}", spec.name))
                        .cloned()
                        .unwrap_or_else(|| {
                            panic!(
                                "missing shard stream {:?} for sharded queue {:?}",
                                format!("{}~{i}", spec.name),
                                spec.name
                            )
                        })
                })
                .collect();
            let local_sharded = Arc::new(ShardedJobQueue::new(shards));
            if let Some(service) = queue_service.as_ref() {
                service.register_sharded_stream(&spec.name, Arc::clone(&local_sharded));
            }
            let client: Arc<dyn JobQueue> = Arc::new(ClusterJobQueue::new(
                &spec.name,
                node_id,
                Arc::clone(&facts) as Arc<dyn ClusterState>,
                Arc::clone(&transport),
            ));
            job_queues.insert(spec.name.clone(), client);
            for i in 0..spec.shard_count {
                job_queues.remove(&format!("{}~{i}", spec.name));
            }
        }

        if let Some(service) = queue_service.as_ref() {
            let mut streams: std::collections::HashSet<String> = self
                .schedule_sources
                .iter()
                .map(|spec| spec.stream.clone())
                .collect();
            for recurring in &self.recurring_jobs {
                streams.insert(recurring.stream.clone());
            }
            for stream in streams {
                let static_jobs: Vec<RecurringJob> = self
                    .recurring_jobs
                    .iter()
                    .filter(|recurring| recurring.stream == stream)
                    .map(|recurring| recurring.job.clone())
                    .collect();
                let user_specs: Vec<&ScheduleSourceSpec> = self
                    .schedule_sources
                    .iter()
                    .filter(|spec| spec.stream == stream)
                    .collect();
                let mut sources: Vec<Arc<dyn ScheduleSource>> = Vec::new();
                let mut poll = Duration::from_secs(60);
                if !static_jobs.is_empty() {
                    sources.push(Arc::new(StaticScheduleSource::new(static_jobs)));
                }
                for spec in user_specs {
                    poll = spec.poll;
                    sources.push(Arc::clone(&spec.source));
                }
                if sources.is_empty() {
                    continue;
                }
                let combined: Arc<dyn ScheduleSource> = if sources.len() == 1 {
                    Arc::clone(&sources[0])
                } else {
                    Arc::new(CompositeScheduleSource::new(sources))
                };
                service.register_schedule_source(stream, combined, poll);
            }
        }

        let topic_service: Option<Arc<TopicService>> = if self.topic_streams.is_empty() {
            None
        } else {
            Some(Arc::new(TopicService::new(
                node_id,
                Arc::clone(&facts) as Arc<dyn ClusterState>,
                Arc::clone(&transport),
            )))
        };
        let mut event_topics: HashMap<String, Arc<dyn EventTopic>> = HashMap::new();
        let mut local_event_topics: HashMap<String, Arc<RedbEventTopic>> = HashMap::new();
        for spec in &self.topic_streams {
            let path = match &spec.path {
                Some(path) => path.clone(),
                None => self
                    .data_dir
                    .as_ref()
                    .unwrap_or_else(|| {
                        panic!(
                            "event_topic({:?}) requires data_dir or event_topic_at with an explicit path",
                            spec.name
                        )
                    })
                    .join(format!("topic-{}.redb", spec.name)),
            };
            let local = Arc::new(
                RedbEventTopic::open(&path, spec.lease_timeout)
                    .unwrap_or_else(|e| panic!("open event topic at {}: {e}", path.display()))
                    .retention(spec.retention),
            );
            if let Some(service) = topic_service.as_ref() {
                service.register_redb_topic(&spec.name, &local);
            }
            let client: Arc<dyn EventTopic> = Arc::new(ClusterEventTopic::new(
                &spec.name,
                node_id,
                Arc::clone(&facts) as Arc<dyn ClusterState>,
                Arc::clone(&transport),
            ));
            event_topics.insert(spec.name.clone(), client);
            local_event_topics.insert(spec.name.clone(), local);
        }
        let topic_bootstrap_specs = self.topic_streams.clone();
        let event_outbox_feeds = self.event_outbox_feeds.clone();
        let event_outbox_cursor: Option<Arc<dyn EventOutboxCursor>> =
            if event_outbox_feeds.is_empty() {
                None
            } else if let Some(data_dir) = self.data_dir.as_ref() {
                Some(Arc::new(
                    RedbEventOutboxCursor::open(data_dir.join("event-outbox-cursors.redb"))
                        .unwrap_or_else(|e| {
                            panic!(
                                "open event outbox cursors at {}: {e}",
                                data_dir.join("event-outbox-cursors.redb").display()
                            )
                        }),
                ))
            } else {
                Some(Arc::new(InMemoryEventOutboxCursor::new()))
            };

        // --- Actor workflow store (redb + voter replication) ----------------
        let mut actor_state_store = self.actor_state_store.clone();
        let store_service: Option<Arc<StoreService>> =
            if actor_state_store.is_none() && self.auto_durable_actor_store {
                self.data_dir.as_ref().map(|data_dir| {
                    let path = data_dir.join("actor-store.redb");
                    let local =
                        Arc::new(RedbActorStateStore::open(&path).unwrap_or_else(|e| {
                            panic!("open actor store at {}: {e}", path.display())
                        }));
                    let service = Arc::new(StoreService::new(
                        node_id,
                        Arc::clone(&local),
                        Arc::clone(&facts) as Arc<dyn ClusterState>,
                        Arc::clone(&transport),
                    ));
                    actor_state_store = Some(Arc::new(ClusterActorStateStore::new(
                        Arc::clone(&local),
                        Arc::clone(&facts) as Arc<dyn ClusterState>,
                        Arc::clone(&transport),
                    )));
                    service
                })
            } else {
                None
            };

        // --- Router -------------------------------------------------------
        let router: Arc<dyn RequestHandler> = Arc::new(NodeRouter::new(
            consensus_service,
            Arc::clone(&control),
            Arc::clone(&messaging),
            Arc::clone(&directory_sync),
            queue_service.clone(),
            topic_service.clone(),
            store_service.clone(),
            Arc::clone(&peers),
            multi_raft.as_ref().map(|state| {
                Arc::new(ArcGroupMigrate(Arc::clone(state))) as Arc<dyn GroupMigratePort>
            }),
        ));

        // --- Background loops --------------------------------------------
        let mut tasks = Vec::new();
        let mut leader_loop_stops: Vec<tokio::sync::watch::Sender<bool>> = Vec::new();

        // Facts refresher + consensus telemetry: mirror consensus status into
        // the supervisor's view, publish Raft gauges / leader + membership
        // events (Track H), prune routing to departed nodes, and trigger an
        // immediate reconcile on any membership or reachability change so joiners
        // get workers without waiting for the periodic timer (E11), a departed
        // or crashed node's managed workers are replaced promptly (E12/liveness-vs-membership),
        {
            let handle = handle.clone();
            let facts = Arc::clone(&facts);
            let directory = Arc::clone(&directory);
            let supervisor = Arc::clone(&supervisor);
            let events = events.clone();
            let period = self.refresh_period;
            // Explicit keepalive: rebalance state must outlive this task.
            let multi_raft_keepalive = multi_raft.clone();
            let meta_for_facts = meta_handle.clone();
            let mut catalog_events = catalog_event_rx;
            let mut telemetry = crate::cluster_handle::MembershipTelemetry::new(
                node_id,
                events.clone(),
                metrics.clone(),
            );
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    if let Some(mr) = multi_raft_keepalive.as_ref()
                        && let Some(ref mut rx) = catalog_events
                    {
                        while let Ok(cmd) = rx.try_recv() {
                            mr.apply_catalog_command(&cmd);
                            if let Ok(report) = mr.rebalance(Arc::clone(&facts)).await {
                                MultiRaftState::<M>::emit_rebalance(&events, &report);
                            }
                        }
                    }
                    let status = if let Some(meta) = meta_for_facts.as_ref() {
                        match meta.status().await {
                            Some(status) => status,
                            None => break,
                        }
                    } else {
                        match handle.status().await {
                            Some(status) => status,
                            None => break,
                        }
                    };
                    facts.update(&status);
                    let delta = telemetry.record(&status);
                    // Prune the local directory copy so routing stops targeting
                    // a departed or unreachable node immediately (every node).
                    for node in delta.departed.iter().chain(&delta.unreachable) {
                        directory.remove_node(*node);
                    }
                    if let Some(mr) = multi_raft_keepalive.as_ref() {
                        // Per-group membership converges incrementally (joint
                        // consensus); retry every tick until the planner is
                        // satisfied (per-group-raft-membership).
                        let _ = mr.sync_group_membership(Arc::clone(&facts)).await;
                        if delta.membership_changed || delta.reachability_changed {
                            let _ = supervisor.reconcile().await;
                            if let Ok(report) = mr.rebalance(Arc::clone(&facts)).await {
                                MultiRaftState::<M>::emit_rebalance(&events, &report);
                            }
                        }
                    } else if delta.membership_changed || delta.reachability_changed {
                        let _ = supervisor.reconcile().await;
                    }
                }
            }));
        }

        // Actor metrics sampler: periodically read the registry's per-group
        // counters and publish rate / latency / mailbox-depth series (Track H).
        // Counting is intrinsic to each instance task (cheap relaxed atomics);
        // this loop is the only place that touches the metrics lock for them.
        {
            let registry = registry.clone();
            let metrics = metrics.clone();
            let events = events.clone();
            let period = self.refresh_period;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                // Last cumulative (messages, handle_nanos) per group, to derive
                // per-interval deltas into the monotonic counters.
                let mut prev: std::collections::HashMap<String, (u64, u64)> =
                    std::collections::HashMap::new();
                loop {
                    interval.tick().await;
                    for stat in registry.stats() {
                        let actor = stat.name.as_str();
                        metrics.set(
                            "trembita_actor_instances",
                            "Live actor instances in a group.",
                            &[("actor", actor)],
                            metric_usize(stat.instances),
                        );
                        metrics.set(
                            "trembita_actor_mailbox_depth",
                            "Queued-but-unhandled messages in a group's mailboxes.",
                            &[("actor", actor)],
                            metric_i64(stat.mailbox_depth),
                        );
                        if stat.mailbox_depth > 0 {
                            let _ = events.emit(TrembitaEvent::MailboxDepth {
                                id: format!("{actor}@n{}", node_id.0),
                                len: stat.mailbox_depth.cast_unsigned(),
                            });
                        }
                        let (pm, pn) = prev.get(actor).copied().unwrap_or((0, 0));
                        let dm = stat.messages.saturating_sub(pm);
                        if dm > 0 {
                            let dn = stat.handle_nanos.saturating_sub(pn);
                            metrics.incr(
                                "trembita_actor_messages_total",
                                "Cumulative messages handled by a group.",
                                &[("actor", actor)],
                                metric_u64(dm),
                            );
                            metrics.incr(
                                "trembita_actor_handle_seconds_total",
                                "Cumulative time spent in a group's handlers.",
                                &[("actor", actor)],
                                metric_u64(dn) / 1e9,
                            );
                        }
                        prev.insert(stat.name, (stat.messages, stat.handle_nanos));
                    }
                }
            }));
        }

        // Directory anti-entropy: republish local registrations to peers.
        {
            let directory_sync = Arc::clone(&directory_sync);
            let registry = registry.clone();
            let members = self.members.clone();
            let period = self.publish_period;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    let rates = registry.group_message_rates();
                    let regs = registry.local_registrations(node_id, Some(&rates));
                    let _ = directory_sync.publish(&members, regs).await;
                }
            }));
        }

        // Peer-address anti-entropy: gossip `/cluster/peers` so nodes learn how
        // to reach members added dynamically via `join` (discovery). Skipped for
        // transports without socket addresses (the in-memory network).
        if let Some(period) = peer_sync {
            let peers = Arc::clone(&peers);
            let transport = Arc::clone(&transport);
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    // Pull each currently known peer's book and merge new
                    // addresses. Converges: the leader (and forwarding follower)
                    // learn a joiner from its join request, and every node then
                    // pulls that address from them.
                    for entry in peers.book().entries {
                        if entry.node == node_id {
                            continue;
                        }
                        if let Ok(remote) = fetch_peers(&*transport, entry.node).await {
                            for peer in remote.entries {
                                peers.learn(peer.node, &peer.addr);
                            }
                        }
                    }
                }
            }));
        }

        // Supervisor reconcile: leader-only placement convergence (supervisor-leader).
        {
            let supervisor = Arc::clone(&supervisor);
            let period = self.reconcile_period;
            let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            leader_loop_stops.push(stop_tx);
            tasks.push(tokio::spawn(async move {
                run_leader_loop(
                    state,
                    LeaderLoopOpts::new(period).with_name("supervisor_reconcile"),
                    stop_rx,
                    move |_| {
                        let supervisor = Arc::clone(&supervisor);
                        async move {
                            let _ = supervisor.reconcile().await;
                        }
                    },
                )
                .await;
            }));
        }

        for spec in self.leader_tasks {
            let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            leader_loop_stops.push(stop_tx);
            let tick = spec.tick;
            let opts = spec.opts;
            tasks.push(tokio::spawn(async move {
                run_leader_loop(state, opts, stop_rx, move |gate| tick(gate)).await;
            }));
        }

        for spawn in self.job_autoscale {
            tasks.push(spawn(
                Arc::clone(&control),
                Arc::clone(&facts) as Arc<dyn ClusterState>,
                Arc::clone(&directory),
                job_queues.clone(),
                Arc::clone(&queue_autoscale_registry),
                Arc::clone(&backlog_registry),
            ));
        }

        for spawn in self.job_membership_autoscale {
            spawn(
                Arc::clone(&facts) as Arc<dyn ClusterState>,
                job_queues.clone(),
                Arc::clone(&queue_autoscale_registry),
                Arc::clone(&backlog_registry),
            );
        }

        if let Some(outbox) = backlog_settle_outbox.as_ref() {
            let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let outbox = Arc::clone(outbox);
            tasks.push(tokio::spawn(async move {
                run_backlog_settle_drainer(
                    Arc::clone(&backlog_registry),
                    outbox,
                    state,
                    BacklogSettleOutboxOpts::default(),
                    stop_rx,
                )
                .await;
            }));
        }

        for feed in &self.backlog_feeds {
            let Some(local) = local_backends.get(&feed.stream).cloned() else {
                panic!(
                    "job_queue_external_backlog stream {:?} has no matching job_queue registration",
                    feed.stream
                );
            };
            let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
            let backlog = Arc::clone(&feed.backlog);
            let stream = feed.stream.clone();
            let opts = feed.opts.clone();
            let settle_outbox = backlog_settle_outbox.clone();
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            tasks.push(tokio::spawn(async move {
                run_backlog_feeder(stream, local, backlog, state, opts, settle_outbox, stop_rx)
                    .await;
            }));
        }

        if messaging.has_mailbox_spool() {
            let messaging = Arc::clone(&messaging);
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            tasks.push(tokio::spawn(async move {
                run_mailbox_spool_drainer(messaging, Duration::from_millis(500), stop_rx).await;
            }));
        }

        if let Some(service) = queue_service.as_ref()
            && service.has_schedule_sources()
        {
            let service = Arc::clone(service);
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            tasks.push(tokio::spawn(async move {
                run_queue_schedule_ticker(service, Duration::from_secs(1), stop_rx).await;
            }));
        }

        if let Some(service) = topic_service.as_ref() {
            let service = Arc::clone(service);
            let specs = topic_bootstrap_specs;
            let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            leader_loop_stops.push(stop_tx);
            let topic_loop = TopicLeaderLoop {
                bootstrapped: false,
                service,
                specs,
            };
            let topic_loop = Arc::new(tokio::sync::Mutex::new(topic_loop));
            tasks.push(tokio::spawn(async move {
                run_leader_loop(
                    state,
                    LeaderLoopOpts::new(Duration::from_millis(200)).with_name("event_topic"),
                    stop_rx,
                    move |gate| {
                        let topic_loop = Arc::clone(&topic_loop);
                        async move {
                            topic_loop.lock().await.tick(gate).await;
                        }
                    },
                )
                .await;
            }));
        }

        if let Some(cursor) = event_outbox_cursor.as_ref() {
            for feed in &event_outbox_feeds {
                let Some(topic) = local_event_topics.get(&feed.topic).cloned() else {
                    panic!(
                        "event_outbox_source topic {:?} has no matching event_topic registration",
                        feed.topic
                    );
                };
                let topic: Arc<dyn EventTopic> = topic;
                let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
                let source = Arc::clone(&feed.source);
                let topic_name = feed.topic.clone();
                let opts = feed.opts.clone();
                let cursor = Arc::clone(cursor);
                let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
                tasks.push(tokio::spawn(async move {
                    run_event_outbox_drainer(
                        topic_name, topic, source, cursor, state, opts, stop_rx,
                    )
                    .await;
                }));
            }
        }

        if let Some(service) = store_service.as_ref() {
            let service = Arc::clone(service);
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            tasks.push(tokio::spawn(async move {
                run_actor_store_gc_ticker(
                    service,
                    DEFAULT_ACTOR_STORE_GC_PERIOD,
                    DEFAULT_ACTOR_STORE_GC_MAX_KEYS,
                    stop_rx,
                )
                .await;
            }));
        }

        let workload = workload_opts.map(|opts| {
            let pool =
                compute_pool.unwrap_or_else(|| ComputeTokenPool::new(opts.max_compute_tokens));
            let connections = Arc::new(ConnectionTracker::default());
            let (tune_tx, tune_rx) = tokio::sync::watch::channel(opts.when_balanced);
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let queues: Vec<Arc<dyn JobQueue>> = job_queues.values().cloned().collect();
            let connections_fn: Arc<dyn Fn() -> usize + Send + Sync> = {
                let c = Arc::clone(&connections);
                Arc::new(move || c.active())
            };
            let metrics_hook = {
                let metrics = metrics.clone();
                Some(Arc::new(move |snap: WorkloadMetricsSnapshot| {
                    metrics.set(
                        "trembita_compute_external_load_units",
                        "Subprocess compute units reported by ExternalLoad on this node.",
                        &[],
                        f64::from(u32::try_from(snap.external_load_units).unwrap_or(u32::MAX)),
                    );
                    metrics.set(
                        "trembita_compute_tokens_in_use",
                        "Compute tokens currently held on this node.",
                        &[],
                        f64::from(u32::try_from(snap.tokens_in_use).unwrap_or(u32::MAX)),
                    );
                    metrics.set(
                        "trembita_compute_token_ceiling",
                        "Effective compute token ceiling after the last governor tick.",
                        &[],
                        f64::from(u32::try_from(snap.token_ceiling).unwrap_or(u32::MAX)),
                    );
                    if snap.tune_changed {
                        metrics.incr(
                            "trembita_workload_tune_events_total",
                            "Consumer tune changes published by the workload governor.",
                            &[],
                            1.0,
                        );
                    }
                }) as trembita_jobs::WorkloadMetricsHook)
            };
            let pool_for_governor = Arc::clone(&pool);
            tasks.push(tokio::spawn(async move {
                run_workload_governor(
                    pool_for_governor,
                    tune_tx,
                    stop_rx,
                    opts,
                    connections_fn,
                    queues,
                    metrics_hook,
                )
                .await;
            }));
            WorkloadRuntime::new(pool, tune_rx, connections, stop_tx)
        });

        let queue_autoscale_proposals: Vec<_> = self.queue_autoscale_meta.into_values().collect();
        if !queue_autoscale_proposals.is_empty() {
            let state = Arc::clone(&facts) as Arc<dyn ClusterState>;
            if let Some(meta) = meta_handle.clone() {
                let proposals = queue_autoscale_proposals.clone();
                tasks.push(tokio::spawn(async move {
                    propose_queue_autoscale_policies(meta, state, proposals).await;
                }));
            } else {
                let h = handle.clone();
                let proposals = queue_autoscale_proposals;
                tasks.push(tokio::spawn(async move {
                    propose_queue_autoscale_policies(h, state, proposals).await;
                }));
            }
        }

        // Admin/observability HTTP server.
        let catalog_version = Arc::new(AtomicU32::new(1));
        if let Some(addr) = self.admin_addr {
            let observer: Arc<dyn Observer> = Arc::new(TrembitaObserver::new(
                node_id,
                handle.clone(),
                Arc::clone(&directory),
                registry.clone(),
                self.shard_count,
                self.shard_routing,
                self.raft_groups,
                self.group_replication_factor,
                self.group_learner_factor,
                multi_raft.clone(),
                Arc::clone(&catalog_version),
                job_queues.clone(),
                Arc::clone(&saga_registry),
                metrics.clone(),
            ));
            let admin = AdminServer::new(observer, metrics.clone(), events.clone());
            match TcpListener::bind(addr).await {
                Ok(listener) => {
                    if let Some(paths) = self.admin_tls.clone() {
                        match admin_tls_config(&paths) {
                            Ok(tls) => {
                                tasks.push(tokio::spawn(async move {
                                    let _ = admin.serve_tls(listener, tls).await;
                                }));
                            }
                            Err(e) => {
                                eprintln!("trembita: admin TLS config failed: {e}");
                            }
                        }
                    } else {
                        tasks.push(tokio::spawn(async move {
                            let _ = admin.serve(listener).await;
                        }));
                    }
                }
                Err(e) => {
                    // A bad admin bind must not take the node down; surface it
                    // and carry on serving the trembita wire.
                    eprintln!("trembita: admin server bind to {addr} failed: {e}");
                }
            }
        }

        let cluster = TrembitaCluster {
            node_id,
            handle,
            group_handles,
            meta_handle,
            raft_groups: self.raft_groups,
            shard_count: self.shard_count,
            shard_routing: self.shard_routing,
            registry,
            control,
            messaging,
            directory,
            directory_sync,
            supervisor,
            events,
            metrics,
            catalog_version,
            saga_registry,
            two_phase_registry,
            queue_autoscale_registry,
            telemetry,
            members: self.members,
            resource_profile,
            vps_resources,
            actor_state_store,
            job_queues,
            event_topics,
            wire_handler: Arc::clone(&router),
            transport,
            facts,
            multi_raft,
            cert_reload: None,
            drain_timeout: self.drain_timeout,
            tasks: Mutex::new(tasks),
            leader_loop_stops,
            workload,
        };
        (cluster, router)
    }
}

async fn propose_queue_autoscale_policies<M: StateMachine>(
    meta: trembita_runtime::NodeHandle<M>,
    state: Arc<dyn ClusterState>,
    proposals: Vec<QueueAutoscalePolicyCommand>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    for _ in 0..200 {
        interval.tick().await;
        if !state.is_leader() {
            continue;
        }
        for command in &proposals {
            let _ = meta.upsert_queue_autoscale_policy(command.clone()).await;
        }
        break;
    }
}

fn upsert_queue_autoscale_meta(
    map: &mut BTreeMap<String, QueueAutoscalePolicyCommand>,
    stream: &str,
    worker: Option<trembita_proto::AutoscalePolicyWire>,
    membership: Option<trembita_proto::MembershipAutoscalePolicyWire>,
) {
    let entry = map
        .entry(stream.to_string())
        .or_insert_with(|| QueueAutoscalePolicyCommand {
            stream: stream.to_string(),
            worker: None,
            membership: None,
        });
    if worker.is_some() {
        entry.worker = worker;
    }
    if membership.is_some() {
        entry.membership = membership;
    }
}

/// Initial voter set before a dynamic join commits (excludes `node_id`).
fn consensus_bootstrap_voters(
    members: &[NodeId],
    node_id: NodeId,
    dynamic_join: bool,
) -> Vec<NodeId> {
    if !dynamic_join {
        return members.to_vec();
    }
    let live: Vec<_> = members
        .iter()
        .copied()
        .filter(|id| *id != node_id)
        .collect();
    if live.is_empty() { vec![node_id] } else { live }
}

/// How long to keep retrying each phase of a dynamic join before giving up.
const JOIN_ATTEMPTS: u32 = 40;
/// Delay between join attempts (≈`JOIN_ATTEMPTS × JOIN_BACKOFF` total budget).
const JOIN_BACKOFF: Duration = Duration::from_millis(250);

/// Drive a dynamic join against a **seed set** (discovery, join-rpc): learn the
/// cluster's peer addresses from whichever seed answers first, then submit a
/// `/cluster/join` (forwarded to the leader) until it commits, the cluster
/// refuses it, or the retry budget is exhausted. Each attempt rotates through
/// every seed so one dead/relocated seed cannot block the join.
async fn join_cluster(
    quic: &Arc<QuicTransport>,
    node_id: NodeId,
    seeds: &[Seed],
    advertise: SocketAddr,
    role: JoinRole,
) -> Result<(), StartError> {
    debug_assert!(!seeds.is_empty(), "join_cluster requires at least one seed");

    // Phase 1: pull the peer-address book from any reachable seed so we can
    // reach the leader (and every member) directly once added.
    let mut booked = false;
    let mut last_err = String::from("no seeds");
    'book: for attempt in 0..JOIN_ATTEMPTS {
        for seed in seeds {
            match fetch_peers(&**quic, seed.node_id).await {
                Ok(book) => {
                    for entry in book.entries {
                        if let Ok(addr) = entry.addr.parse::<SocketAddr>() {
                            quic.learn_peer(entry.node, addr);
                        }
                    }
                    booked = true;
                    break 'book;
                }
                Err(e) => last_err = e.to_string(),
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    if !booked {
        return Err(StartError::Join(format!(
            "no seed reachable to bootstrap discovery: {last_err}"
        )));
    }

    // Phase 2: ask to join. Any seed forwards to the leader on our behalf, so a
    // `Redirect` means "no leader yet" — retry against the next seed.
    let request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: Some(node_id),
        advertise_addr: advertise.to_string(),
        role,
    };
    for attempt in 0..JOIN_ATTEMPTS {
        let last = attempt + 1 == JOIN_ATTEMPTS;
        for (i, seed) in seeds.iter().enumerate() {
            let last_seed = last && i + 1 == seeds.len();
            match send_join_request(&**quic, seed.node_id, &request).await {
                // A restart of an already-joined node is a no-op, not a failure.
                Ok(
                    JoinResponse::Accepted { .. }
                    | JoinResponse::Rejected {
                        reason: JoinRejection::Duplicate,
                    },
                ) => return Ok(()),
                Ok(JoinResponse::Rejected { reason }) => {
                    return Err(StartError::Join(format!(
                        "cluster rejected join: {reason:?}"
                    )));
                }
                Ok(JoinResponse::Redirect { leader }) if last_seed => {
                    return Err(StartError::Join(format!(
                        "no leader available to accept the join (hint: {leader:?})"
                    )));
                }
                Err(e) if last_seed => {
                    return Err(StartError::Join(format!("join request failed: {e}")));
                }
                Ok(JoinResponse::Redirect { .. }) | Err(_) => {}
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    Err(StartError::Join(
        "join did not commit before the retry budget elapsed".to_string(),
    ))
}

/// Pre-join handshake: leader assigns a node id before this node starts Raft.
async fn join_cluster_auto(
    quic: &Arc<QuicTransport>,
    seeds: &[Seed],
    advertise: SocketAddr,
    role: JoinRole,
) -> Result<(NodeId, Membership), StartError> {
    debug_assert!(
        !seeds.is_empty(),
        "join_cluster_auto requires at least one seed"
    );

    let mut booked = false;
    let mut last_err = String::from("no seeds");
    'book: for attempt in 0..JOIN_ATTEMPTS {
        for seed in seeds {
            match fetch_peers(&**quic, seed.node_id).await {
                Ok(book) => {
                    for entry in book.entries {
                        if let Ok(addr) = entry.addr.parse::<SocketAddr>() {
                            quic.learn_peer(entry.node, addr);
                        }
                    }
                    booked = true;
                    break 'book;
                }
                Err(e) => last_err = e.to_string(),
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    if !booked {
        return Err(StartError::Join(format!(
            "no seed reachable to bootstrap discovery: {last_err}"
        )));
    }

    let request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: None,
        advertise_addr: advertise.to_string(),
        role,
    };
    for attempt in 0..JOIN_ATTEMPTS {
        let last = attempt + 1 == JOIN_ATTEMPTS;
        for (i, seed) in seeds.iter().enumerate() {
            let last_seed = last && i + 1 == seeds.len();
            match send_join_request(&**quic, seed.node_id, &request).await {
                Ok(JoinResponse::Accepted {
                    node_id,
                    membership,
                    ..
                }) => return Ok((node_id, membership)),
                Ok(JoinResponse::Rejected { reason }) => {
                    return Err(StartError::Join(format!(
                        "cluster rejected auto join: {reason:?}"
                    )));
                }
                Ok(JoinResponse::Redirect { leader }) if last_seed => {
                    return Err(StartError::Join(format!(
                        "no leader available to assign node id (hint: {leader:?})"
                    )));
                }
                Err(e) if last_seed => {
                    return Err(StartError::Join(format!("auto join request failed: {e}")));
                }
                Ok(JoinResponse::Redirect { .. }) | Err(_) => {}
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    Err(StartError::Join(
        "auto join did not commit before the retry budget elapsed".to_string(),
    ))
}

#[cfg(all(test, feature = "dev-certs"))]
mod merge_app_config_tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use trembita_net::{LocalNetwork, PeerDirectory};
    use trembita_runtime::DEFAULT_DRAIN_TIMEOUT;

    use super::*;
    use crate::app::EmptyStateMachine;
    use crate::env_config::{AppConfig, EnvOverrides};
    use crate::security::Security;

    fn test_app_config(node_id: NodeId) -> AppConfig {
        let ca = trembita_net::tls::ClusterCa::generate().expect("ca");
        AppConfig {
            node_id,
            listen: "127.0.0.1:7443".parse::<SocketAddr>().expect("addr"),
            admin: None,
            admin_tls: None,
            peers: PeerDirectory::new(),
            members: Vec::new(),
            join_seeds: Vec::new(),
            allow_join: true,
            allow_voter_join: false,
            join_role: JoinRole::Learner,
            allow_leave: true,
            graceful_leave: true,
            voter_replacement: true,
            voter_replacement_grace_ticks: None,
            security: Security::dev(&ca, node_id).expect("security"),
            pem_paths: None,
            cert_dir: None,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            cert_watch: Duration::from_secs(60),
            data_dir: None,
            job_queue_stream: None,
            job_queue_lease: Duration::from_secs(60),
            gateway: None,
            gateway_jobs_api: false,
            gateway_actors_api: false,
            gateway_workflows_api: false,
            gateway_introspect_api: false,
            gateway_drain_timeout: crate::gateway::DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            gateway_tls: None,
            env: EnvOverrides::default(),
        }
    }

    #[tokio::test]
    async fn merge_app_config_applies_node_id_from_env_cfg() {
        let net = LocalNetwork::new();
        let cluster = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .merge_app_config(&test_app_config(NodeId(7)))
            .tick_period(Duration::from_millis(5))
            .start_local(&net)
            .await;
        assert_eq!(cluster.node_id(), NodeId(7));

        let joiner = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .merge_app_config(&test_app_config(NodeId(0)))
            .tick_period(Duration::from_millis(5))
            .start_local(&LocalNetwork::new())
            .await;
        assert_eq!(joiner.node_id(), NodeId(0));
    }

    #[test]
    fn merge_app_config_applies_join_role_and_allow_voter_join_from_env() {
        let mut cfg = test_app_config(NodeId(2));
        cfg.join_role = JoinRole::Voter;
        cfg.allow_voter_join = true;
        let builder =
            TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine).merge_app_config(&cfg);
        assert_eq!(builder.join_role, JoinRole::Voter);
        assert!(builder.runtime.allow_voter_join);
    }

    #[test]
    fn merge_app_config_code_join_role_wins_over_env() {
        let mut cfg = test_app_config(NodeId(2));
        cfg.join_role = JoinRole::Voter;
        let builder = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .join_as(JoinRole::Learner)
            .merge_app_config(&cfg);
        assert_eq!(builder.join_role, JoinRole::Learner);
    }

    #[test]
    fn merge_app_config_explicit_node_id_wins_over_env() {
        let builder = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine)
            .with_explicit_node_id()
            .merge_app_config(&test_app_config(NodeId(7)));
        assert_eq!(builder.node_id, NodeId(1));
    }

    #[test]
    fn merge_app_config_members_from_env_only_when_peers_set() {
        let mut cfg = test_app_config(NodeId(3));
        cfg.members = vec![NodeId(1), NodeId(2), NodeId(3)];
        cfg.env.peers = false;
        let builder = TrembitaClusterBuilder::new(NodeId(5), EmptyStateMachine)
            .members([NodeId(5)])
            .merge_app_config(&cfg);
        assert_eq!(builder.members, vec![NodeId(5)]);

        cfg.env.peers = true;
        let builder =
            TrembitaClusterBuilder::new(NodeId(5), EmptyStateMachine).merge_app_config(&cfg);
        assert_eq!(builder.members, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }
}
