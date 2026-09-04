//! [`TrembitaClusterBuilder`] — the single ergonomic entry point (deployment-model,
//! library-and-publishing). Describe a node (its id, membership, state machine, actor types, and
//! managed groups), then `start_*` it over a transport; the builder assembles
//! the consensus runtime, the actor control/messaging/directory planes, the
//! leader-only supervisor, telemetry, and the admin server, and wires the
//! background loops that keep them current.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use trembita_core::{
    Config, DEFAULT_GROUP_LEARNER_FACTOR, DEFAULT_GROUP_REPLICATION_FACTOR, ReachabilityConfig,
    StateMachine,
};
use trembita_dashboard::{AdminTlsPaths, MetricsSink};
use trembita_jobs::WorkloadOpts;
use trembita_net::TrafficPolicy;
use trembita_proto::{JoinRole, NodeId, QueueAutoscalePolicyCommand};
use trembita_runtime::{
    ClusterControl, ClusterSupervisor, DEFAULT_DRAIN_TIMEOUT, DirectoryPolicy, DirectoryRetry,
    LeaderGate, LeaderLoopOpts, ResourceProfile, RuntimeConfig, UserActor,
};

use crate::cluster_handle::ClusterFacts;
use crate::discovery::Seed;

mod assemble;
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
    ///
    /// # Panics
    /// Panics when `count < 1`.
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
            self.members.clone_from(&cfg.members);
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
            self.join_seeds.clone_from(&cfg.join_seeds);
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
}
