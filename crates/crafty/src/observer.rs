//! The [`Observer`] the admin server reads from (health-admin-port readiness, observability §4
//! introspection), backed by a live node: consensus status comes from the
//! [`NodeHandle`], the actor picture from the shared [`ActorDirectory`] and the
//! local [`ActorRegistry`] (mailbox depth + uptime for actors hosted here; remote
//! stats arrive via directory anti-entropy).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crafty_core::{Role, StateMachine};
use crafty_dashboard::{
    ActorView, BoxFuture, ClusterView, Metrics, NodeSummary, NodeView, Observer, QueueStreamView,
    QueuesView, RaftGroupSummary, RaftGroupsView, Readiness, SagaRecordView,
};
use crafty_proto::NodeId;

use crafty_actor::{ActorDirectory, ActorRegistry, JobQueue, NodeHandle};
use crafty_client::SagaJournalPhase;

use crate::multi_raft::MultiRaftState;
use crate::saga::SagaRegistry;

/// A read-only view onto one running node for the admin/dashboard endpoints.
pub(crate) struct CraftyObserver<M: StateMachine> {
    node_id: NodeId,
    handle: NodeHandle<M>,
    directory: Arc<ActorDirectory>,
    registry: ActorRegistry,
    shard_count: u32,
    shard_routing: crafty_core::ShardRoutingKind,
    raft_groups: u32,
    replication_factor: u32,
    learner_factor: u32,
    multi_raft: Option<Arc<MultiRaftState<M>>>,
    catalog_version: Arc<AtomicU32>,
    job_queues: HashMap<String, Arc<dyn JobQueue>>,
    saga_registry: SagaRegistry,
    metrics: Metrics,
}

impl<M: StateMachine> CraftyObserver<M> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node_id: NodeId,
        handle: NodeHandle<M>,
        directory: Arc<ActorDirectory>,
        registry: ActorRegistry,
        shard_count: u32,
        shard_routing: crafty_core::ShardRoutingKind,
        raft_groups: u32,
        replication_factor: u32,
        learner_factor: u32,
        multi_raft: Option<Arc<MultiRaftState<M>>>,
        catalog_version: Arc<AtomicU32>,
        job_queues: HashMap<String, Arc<dyn JobQueue>>,
        saga_registry: SagaRegistry,
        metrics: Metrics,
    ) -> Self {
        Self {
            node_id,
            handle,
            directory,
            registry,
            shard_count,
            shard_routing,
            raft_groups,
            replication_factor,
            learner_factor,
            multi_raft,
            catalog_version,
            job_queues,
            saga_registry,
            metrics,
        }
    }
}

/// A stable, cluster-unique id string for an actor instance.
fn actor_key(reg: &crafty_proto::ActorRegistration) -> String {
    format!("{}/{}#{}", reg.id.node.0, reg.id.name, reg.id.instance)
}

fn role_str(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::PreCandidate => "pre_candidate",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}

fn saga_phase_str(phase: SagaJournalPhase) -> &'static str {
    match phase {
        SagaJournalPhase::Running => "running",
        SagaJournalPhase::Completed => "completed",
        SagaJournalPhase::Compensating => "compensating",
        SagaJournalPhase::Compensated => "compensated",
        SagaJournalPhase::Stuck => "stuck",
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; queue depths fit in practice.
fn metric_u64(v: u64) -> f64 {
    v as f64
}

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; saga counts fit in practice.
fn metric_usize(v: usize) -> f64 {
    v as f64
}

fn mailbox_depth_u64(depth: i64) -> u64 {
    depth.max(0).cast_unsigned()
}

impl<M: StateMachine> Observer for CraftyObserver<M> {
    fn readiness(&self) -> BoxFuture<'_, Readiness> {
        Box::pin(async move {
            let Some(status) = self.handle.status().await else {
                return Readiness {
                    node_id: self.node_id.0,
                    role: "stopped".to_string(),
                    member: false,
                    draining: true,
                    workers: Vec::new(),
                    reason: Some("runtime stopped".to_string()),
                };
            };
            let member = status.voters.contains(&self.node_id);
            Readiness {
                node_id: self.node_id.0,
                role: role_str(status.role).to_string(),
                member,
                draining: false,
                workers: self.registry.names(),
                reason: (!member).then(|| "not a cluster member yet".to_string()),
            }
        })
    }

    fn cluster(&self) -> BoxFuture<'_, ClusterView> {
        Box::pin(async move {
            let Some(status) = self.handle.status().await else {
                return ClusterView {
                    leader: None,
                    term: 0,
                    commit_index: 0,
                    nodes: Vec::new(),
                };
            };
            let leader = status.leader;
            let nodes = status
                .voters
                .iter()
                .map(|id| {
                    let role = if *id == self.node_id {
                        role_str(status.role)
                    } else if Some(*id) == leader {
                        "leader"
                    } else {
                        "follower"
                    };
                    NodeSummary {
                        id: id.0,
                        role: role.to_string(),
                        member: true,
                    }
                })
                .collect();
            ClusterView {
                leader: leader.map(|n| n.0),
                term: status.term.0,
                commit_index: status.commit_index.0,
                nodes,
            }
        })
    }

    fn raft_groups(&self) -> BoxFuture<'_, RaftGroupsView> {
        Box::pin(async move {
            let active_shard_count = self
                .multi_raft
                .as_ref()
                .map_or(self.shard_count, |mr| mr.sharded.shard_count());
            let shard_routing = self
                .multi_raft
                .as_ref()
                .map_or(self.shard_routing, |mr| mr.sharded.routing_kind());
            let hosted: Vec<u32> = self
                .multi_raft
                .as_ref()
                .map(|mr| mr.handles.lock().unwrap().keys().copied().collect())
                .unwrap_or_default();
            let mut groups = Vec::new();
            if let Some(mr) = &self.multi_raft {
                let snapshots: Vec<(u32, NodeHandle<M>)> = {
                    let handles = mr.handles.lock().unwrap();
                    let catalog = mr.catalog.lock().unwrap();
                    catalog
                        .iter()
                        .filter_map(|group| {
                            handles
                                .get(&group.0)
                                .map(|handle| (group.0, handle.clone()))
                        })
                        .collect()
                };
                for (group_id, handle) in snapshots {
                    if let Some(status) = handle.status().await {
                        groups.push(RaftGroupSummary {
                            group_id,
                            role: role_str(status.role).to_string(),
                            leader: status.leader.map(|n| n.0),
                            term: status.term.0,
                            commit_index: status.commit_index.0,
                            voters: status.voters.iter().map(|n| n.0).collect(),
                            learners: status.learners.iter().map(|n| n.0).collect(),
                            hosted_on_this_node: true,
                        });
                    }
                }
            } else if self.raft_groups <= 1
                && let Some(status) = self.handle.status().await
            {
                groups.push(RaftGroupSummary {
                    group_id: 0,
                    role: role_str(status.role).to_string(),
                    leader: status.leader.map(|n| n.0),
                    term: status.term.0,
                    commit_index: status.commit_index.0,
                    voters: status.voters.iter().map(|n| n.0).collect(),
                    learners: status.learners.iter().map(|n| n.0).collect(),
                    hosted_on_this_node: true,
                });
            }
            RaftGroupsView {
                shard_count: active_shard_count,
                shard_routing: shard_routing.as_str().into(),
                catalog_size: self.multi_raft.as_ref().map_or(self.raft_groups, |mr| {
                    u32::try_from(mr.catalog.lock().unwrap().len()).unwrap_or(u32::MAX)
                }),
                catalog_version: self.catalog_version.load(Ordering::SeqCst),
                replication_factor: self.replication_factor,
                learner_factor: self.learner_factor,
                hosted_groups: hosted,
                groups,
            }
        })
    }

    fn actors(&self) -> BoxFuture<'_, Vec<ActorView>> {
        Box::pin(async move {
            let group_rates = self.registry.group_message_rates();
            let local: HashMap<(String, u32), (u64, u64)> = self
                .registry
                .local_actor_introspection()
                .into_iter()
                .map(|i| {
                    (
                        (i.name, i.instance),
                        (mailbox_depth_u64(i.mailbox_depth), i.uptime_secs),
                    )
                })
                .collect();

            let mut out = Vec::new();
            for name in self.directory.groups() {
                for reg in self.directory.lookup(&name) {
                    let (mailbox_depth, uptime_secs) = if reg.id.node == self.node_id {
                        local
                            .get(&(reg.id.name.clone(), reg.id.instance))
                            .copied()
                            .unwrap_or((reg.mailbox_depth, reg.uptime_secs))
                    } else {
                        (reg.mailbox_depth, reg.uptime_secs)
                    };
                    let messages_per_sec = if reg.id.node == self.node_id {
                        *group_rates.get(&reg.id.name).unwrap_or(&0.0)
                    } else {
                        reg.messages_per_sec
                    };
                    out.push(ActorView {
                        id: actor_key(&reg),
                        node: reg.id.node.0,
                        actor_type: reg.actor_type.0.clone(),
                        mailbox_depth,
                        uptime_secs,
                        generation: u32::try_from(reg.id.generation).unwrap_or(u32::MAX),
                        messages_per_sec,
                    });
                }
            }
            out
        })
    }

    fn actor(&self, id: &str) -> BoxFuture<'_, Option<ActorView>> {
        let id = id.to_string();
        Box::pin(async move { self.actors().await.into_iter().find(|a| a.id == id) })
    }

    fn node(&self, id: u64) -> BoxFuture<'_, Option<NodeView>> {
        Box::pin(async move {
            let target = NodeId(id);
            let mut workers: Vec<String> = self
                .directory
                .groups()
                .into_iter()
                .filter(|name| {
                    self.directory
                        .lookup(name)
                        .iter()
                        .any(|reg| reg.id.node == target)
                })
                .collect();
            if target == self.node_id {
                for name in self.registry.names() {
                    if !workers.contains(&name) {
                        workers.push(name);
                    }
                }
            }
            workers.sort();
            workers.dedup();

            let is_voter = self
                .handle
                .status()
                .await
                .is_some_and(|s| s.voters.contains(&target));
            if !is_voter && workers.is_empty() {
                return None;
            }
            Some(NodeView {
                id,
                workers,
                cpus: 0,
                store_healthy: true,
            })
        })
    }

    fn queues(&self) -> BoxFuture<'_, QueuesView> {
        let queues = self.job_queues.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let mut streams = Vec::new();
            for (stream, queue) in queues {
                if let Ok(m) = queue.metrics().await {
                    let oldest_pending_age_ms =
                        u64::try_from(m.oldest_pending_age.as_millis()).unwrap_or(u64::MAX);
                    metrics.set(
                        "crafty_queue_pending",
                        "Pending jobs eligible to lease.",
                        &[("stream", &stream)],
                        metric_u64(m.pending),
                    );
                    metrics.set(
                        "crafty_queue_leased",
                        "Jobs currently leased.",
                        &[("stream", &stream)],
                        metric_u64(m.leased),
                    );
                    metrics.set(
                        "crafty_queue_dead_letter",
                        "Jobs in dead letter.",
                        &[("stream", &stream)],
                        metric_u64(m.dead_letter),
                    );
                    metrics.set(
                        "crafty_queue_oldest_pending_age_ms",
                        "Age of oldest ready pending job.",
                        &[("stream", &stream)],
                        metric_u64(oldest_pending_age_ms),
                    );
                    streams.push(QueueStreamView {
                        stream,
                        pending: m.pending,
                        leased: m.leased,
                        dead_letter: m.dead_letter,
                        oldest_pending_age_ms,
                    });
                }
            }
            streams.sort_by(|a, b| a.stream.cmp(&b.stream));
            QueuesView { streams }
        })
    }

    fn sagas(&self) -> BoxFuture<'_, Vec<SagaRecordView>> {
        let registry = Arc::clone(&self.saga_registry);
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let records = registry.lock().expect("lock");
            let active = records
                .values()
                .filter(|r| {
                    matches!(
                        r.phase,
                        SagaJournalPhase::Running | SagaJournalPhase::Compensating
                    )
                })
                .count();
            metrics.set(
                "crafty_saga_active",
                "Sagas in running or compensating phase.",
                &[],
                metric_usize(active),
            );
            let mut out: Vec<SagaRecordView> = records
                .values()
                .map(|r| SagaRecordView {
                    saga_id: hex_bytes(&r.saga_id),
                    phase: saga_phase_str(r.phase).to_string(),
                    completed_steps: r.completed_steps,
                    catalog_version: r.catalog_version,
                    failed_step: r.failed_step,
                    compensate_failed_at: r.compensate_failed_at,
                })
                .collect();
            out.sort_by(|a, b| a.saga_id.cmp(&b.saga_id));
            out
        })
    }
}
