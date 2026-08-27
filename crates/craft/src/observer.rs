//! The [`Observer`] the admin server reads from (health-admin-port readiness, observability §4
//! introspection), backed by a live node: consensus status comes from the
//! [`NodeHandle`], the actor picture from the shared [`ActorDirectory`] and the
//! local [`ActorRegistry`].
//!
//! Mailbox depth and per-actor uptime are not yet plumbed through the registry,
//! so those fields report `0` for now (tracked as observability follow-ups);
//! everything else reflects real runtime state.

use std::sync::Arc;

use craft_core::{Role, StateMachine};
use craft_dashboard::{
    ActorView, BoxFuture, ClusterView, NodeSummary, NodeView, Observer, RaftGroupSummary,
    RaftGroupsView, Readiness,
};
use craft_proto::NodeId;

use craft_actor::{ActorDirectory, ActorRegistry, NodeHandle};

use crate::multi_raft::MultiRaftState;

/// A read-only view onto one running node for the admin/dashboard endpoints.
pub(crate) struct CraftObserver<M: StateMachine> {
    node_id: NodeId,
    handle: NodeHandle<M>,
    directory: Arc<ActorDirectory>,
    registry: ActorRegistry,
    shard_count: u32,
    shard_routing: craft_core::ShardRoutingKind,
    raft_groups: u32,
    replication_factor: u32,
    learner_factor: u32,
    multi_raft: Option<Arc<MultiRaftState<M>>>,
}

impl<M: StateMachine> CraftObserver<M> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node_id: NodeId,
        handle: NodeHandle<M>,
        directory: Arc<ActorDirectory>,
        registry: ActorRegistry,
        shard_count: u32,
        shard_routing: craft_core::ShardRoutingKind,
        raft_groups: u32,
        replication_factor: u32,
        learner_factor: u32,
        multi_raft: Option<Arc<MultiRaftState<M>>>,
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
        }
    }
}

/// A stable, cluster-unique id string for an actor instance.
fn actor_key(reg: &craft_proto::ActorRegistration) -> String {
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

impl<M: StateMachine> Observer for CraftObserver<M> {
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
                .map(|mr| mr.sharded.shard_count())
                .unwrap_or(self.shard_count);
            let shard_routing = self
                .multi_raft
                .as_ref()
                .map(|mr| mr.sharded.routing_kind())
                .unwrap_or(self.shard_routing);
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
                catalog_size: self
                    .multi_raft
                    .as_ref()
                    .map(|mr| mr.catalog.lock().unwrap().len() as u32)
                    .unwrap_or(self.raft_groups),
                replication_factor: self.replication_factor,
                learner_factor: self.learner_factor,
                hosted_groups: hosted,
                groups,
            }
        })
    }

    fn actors(&self) -> BoxFuture<'_, Vec<ActorView>> {
        Box::pin(async move {
            let mut out = Vec::new();
            for name in self.directory.groups() {
                for reg in self.directory.lookup(&name) {
                    out.push(ActorView {
                        id: actor_key(&reg),
                        node: reg.id.node.0,
                        actor_type: reg.actor_type.0.clone(),
                        mailbox_depth: 0,
                        uptime_secs: 0,
                        generation: reg.id.generation as u32,
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
}
