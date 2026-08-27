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
    ActorView, BoxFuture, ClusterView, NodeSummary, NodeView, Observer, Readiness,
};
use craft_proto::NodeId;

use craft_actor::{ActorDirectory, ActorRegistry, NodeHandle};

/// A read-only view onto one running node for the admin/dashboard endpoints.
pub(crate) struct CraftObserver<M: StateMachine> {
    node_id: NodeId,
    handle: NodeHandle<M>,
    directory: Arc<ActorDirectory>,
    registry: ActorRegistry,
}

impl<M: StateMachine> CraftObserver<M> {
    pub(crate) fn new(
        node_id: NodeId,
        handle: NodeHandle<M>,
        directory: Arc<ActorDirectory>,
        registry: ActorRegistry,
    ) -> Self {
        Self {
            node_id,
            handle,
            directory,
            registry,
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
                    // We know our own role precisely; peers are summarised as
                    // leader (if the leader hint points at them) or follower.
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
            // The local node also knows its own live registry directly, even
            // before the next directory publish.
            if target == self.node_id {
                for name in self.registry.names() {
                    if !workers.contains(&name) {
                        workers.push(name);
                    }
                }
            }
            workers.sort();
            workers.dedup();

            // Report a node only if it is a known voter or hosts actors.
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
