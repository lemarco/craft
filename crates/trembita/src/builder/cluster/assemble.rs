//! Assemble runtime components and spawn background loops.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use trembita_core::{RaftNode, StateMachine};
use trembita_dashboard::{
    AdminServer, EventBus, Metrics, Observer, TrembitaEvent, admin_tls_config,
};
use trembita_net::Transport;
use trembita_net::transport::RequestHandler;
use trembita_proto::{CatalogCommand, NodeId, QueueAutoscalePolicyCommand};
use trembita_storage::GroupRedbLayout;

use trembita_actor_store::{
    ClusterActorStateStore, DEFAULT_ACTOR_STORE_GC_MAX_KEYS, DEFAULT_ACTOR_STORE_GC_PERIOD,
    RedbActorStateStore, StoreService, run_actor_store_gc_ticker,
};
use trembita_events::{
    ClusterEventTopic, EventOutboxCursor, EventOutboxPoll, EventTopic, InMemoryEventOutboxCursor,
    RedbEventOutboxCursor, RedbEventTopic, TopicService, run_event_outbox_drainer,
};
use trembita_jobs::{
    BacklogSettleOutbox, BacklogSettleOutboxOpts, ClusterJobQueue, CompositeScheduleSource,
    InMemoryBacklogSettleOutbox, JobQueue, QueueService, RedbBacklogSettleOutbox, RedbJobQueue,
    SchedulePoll, ShardedJobQueue, StaticScheduleSource, WorkloadMetricsSnapshot, WorkloadOpts,
    run_backlog_feeder, run_backlog_settle_drainer, run_queue_autoscaler,
    run_queue_membership_autoscaler, run_queue_schedule_ticker, run_workload_governor,
};
use trembita_runtime::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterMessaging, ClusterState,
    ClusterSupervisor, ComputeTokenPool, DirectorySync, MailboxSpool, NodeService, RaftDriver,
    RedbMailboxSpool, ResourceProfile, run_leader_loop, run_mailbox_spool_drainer,
    spawn_multi_raft_node, spawn_node,
};

use crate::certs::{CertReloadHandle, cert_paths_for_node};
use crate::cluster_handle::{ClusterFacts, TrembitaCluster};
use crate::gateway::ConnectionTracker;
use crate::handler::{NodeRouter, QuicPeers};
use crate::multi_raft::{ArcGroupMigrate, GroupMigratePort, MultiRaftState};
use crate::node_id;
use crate::observer::TrembitaObserver;
use crate::workload::WorkloadRuntime;

use super::TrembitaClusterBuilder;
use super::topic_leader::TopicLeaderLoop;
use super::types::{
    AutoscaleTask, BacklogFeedSpec, EventOutboxFeedSpec, JobStreamSpec, ManageFn, RegisterFn,
    ScheduleSourceSpec, ShardedJobSpec, TopicStreamSpec, UserLeaderTaskSpec,
};
use crate::builder::autoscale::{propose_queue_autoscale_policies, upsert_queue_autoscale_meta};
use crate::builder::join::consensus_bootstrap_voters;
use trembita_net::fetch_peers;
use trembita_runtime::{LeaderLoopOpts, VpsResources};

#[allow(clippy::cast_precision_loss)]
fn metric_usize(v: usize) -> f64 {
    v as f64
}
#[allow(clippy::cast_precision_loss)]
fn metric_i64(v: i64) -> f64 {
    v as f64
}
#[allow(clippy::cast_precision_loss)]
fn metric_u64(v: u64) -> f64 {
    v as f64
}

impl<M: trembita_core::StateMachine + Default + 'static> TrembitaClusterBuilder<M> {
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
            let topic_loop = TopicLeaderLoop::new(service, specs);
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
