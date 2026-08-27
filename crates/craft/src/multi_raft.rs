//! Multi-Raft rebalance execution for the facade (write-sharding-multi-raft).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use craft_actor::{
    ClusterState, GroupMembershipSyncReport, GroupRebalanceReport, NodeHandle, RaftGroupReconciler,
    RuntimeConfig, ShardedNodeService, spawn_raft_group, spawn_raft_group_from_bundle,
    sync_hosted_group_membership,
};
use craft_core::{Config, StateMachine};
use craft_core::{RaftGroupId, group_voters};
use craft_dashboard::CraftEvent;
use craft_net::{Transport, send_group_migrate};
use craft_proto::{GroupMigrateReply, GroupMigrateRequest, NodeId};
use craft_storage::StorageError;

use crate::cluster::ClusterFacts;

/// Runtime state for dynamic multi-Raft group hosting on one physical node.
pub(crate) struct MultiRaftState<M: StateMachine> {
    pub sharded: Arc<ShardedNodeService>,
    pub handles: Mutex<BTreeMap<u32, NodeHandle<M>>>,
    pub transport: Arc<dyn Transport>,
    pub raft: Config,
    pub runtime: RuntimeConfig,
    pub forward_timeout: std::time::Duration,
    pub data_dir: Option<PathBuf>,
    pub catalog: Vec<RaftGroupId>,
    pub node_id: NodeId,
    /// Per-group voter replication factor (per-group-raft-membership).
    pub replication_factor: u32,
    /// Non-voting learners per group beyond voters (Tier 1).
    pub learner_factor: u32,
}

impl<M: StateMachine + Default + 'static> MultiRaftState<M> {
    fn voters_for_group(&self, group: u32, live: &[NodeId]) -> Vec<NodeId> {
        group_voters(RaftGroupId(group), live, self.replication_factor)
    }

    /// For each hosted group where this node is leader, propose the planner's
    /// desired voter set (per-group-raft-membership Phase 2).
    pub async fn sync_group_membership(
        &self,
        facts: Arc<ClusterFacts>,
    ) -> GroupMembershipSyncReport {
        let live = ClusterState::live_nodes(facts.as_ref());
        let hosted: Vec<_> = {
            let handles = self.handles.lock().unwrap();
            handles
                .iter()
                .map(|(id, handle)| (*id, handle.clone()))
                .collect()
        };
        sync_hosted_group_membership(
            &hosted,
            &live,
            &self.catalog,
            self.replication_factor,
            self.learner_factor,
        )
        .await
    }

    /// Plan and apply local adopt/retire actions, pushing retired groups to
    /// their new host over the wire when rendezvous placement moves them.
    pub async fn rebalance(
        &self,
        facts: Arc<ClusterFacts>,
    ) -> Result<GroupRebalanceReport, StorageError> {
        let hosted = self.sharded.hosted_group_ids();
        let reconciler = RaftGroupReconciler::new(
            self.node_id,
            self.catalog.clone(),
            self.replication_factor,
            Arc::clone(&facts),
        );
        let report = reconciler.reconcile_local(&hosted);
        if report.plan.adopt.is_empty() && report.plan.retire.is_empty() {
            return Ok(report);
        }

        craft_actor::rebalance_log::line(format!(
            "node={} applying adopt={:?} retire={:?}",
            self.node_id.0,
            report.plan.adopt.iter().map(|g| g.0).collect::<Vec<_>>(),
            report.plan.retire.iter().map(|g| g.0).collect::<Vec<_>>(),
        ));

        for group in &report.plan.retire {
            let id = group.0;
            let Some(target) = report.assignment.get(group).copied() else {
                continue;
            };
            if target == self.node_id {
                continue;
            }
            let handle = self.handles.lock().unwrap().get(&id).cloned();
            let Some(handle) = handle else {
                continue;
            };
            craft_actor::rebalance_log::line(format!(
                "node={} migrate group={id} -> {}",
                self.node_id.0, target.0
            ));
            let bundle = handle
                .export_migration()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let reply = send_group_migrate(
                self.transport.as_ref(),
                target,
                &GroupMigrateRequest {
                    group: id,
                    from: self.node_id,
                    bundle,
                },
            )
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
            if !reply.adopted {
                return Err(StorageError::Backend(format!(
                    "group {id} migration to {} failed: {}",
                    target.0,
                    reply.error.unwrap_or_else(|| "unknown".into())
                )));
            }
        }

        let mut handles = self.handles.lock().unwrap();
        let live = ClusterState::live_nodes(facts.as_ref());
        for group in &report.plan.retire {
            let id = group.0;
            craft_actor::rebalance_log::line(format!("node={} retire group={id}", self.node_id.0));
            if let Some(handle) = handles.remove(&id) {
                handle.shutdown();
                self.sharded.remove_group(id);
            }
        }

        for group in &report.plan.adopt {
            let id = group.0;
            if handles.contains_key(&id) {
                craft_actor::rebalance_log::line(format!(
                    "node={} adopt group={id} skipped (already hosted)",
                    self.node_id.0
                ));
                continue;
            }
            craft_actor::rebalance_log::line(format!("node={} adopt group={id}", self.node_id.0));
            let voters = self.voters_for_group(id, &live);
            let (service, handle) = spawn_raft_group(
                self.node_id,
                &voters,
                id,
                self.raft.clone(),
                self.runtime.clone(),
                M::default(),
                Arc::clone(&self.transport),
                self.forward_timeout,
                self.data_dir.as_deref(),
            )?;
            self.sharded.insert_group(id, service);
            handles.insert(id, handle);
        }

        Ok(report)
    }

    /// Adopt a group replica shipped from another physical node.
    pub fn adopt_group_migrate(
        &self,
        request: &GroupMigrateRequest,
        live_nodes: &[NodeId],
    ) -> Result<GroupMigrateReply, StorageError> {
        let id = request.group;
        if !self.catalog.iter().any(|g| g.0 == id) {
            return Ok(GroupMigrateReply {
                adopted: false,
                error: Some(format!("unknown raft group {id}")),
            });
        }

        let mut handles = self.handles.lock().unwrap();
        if handles.contains_key(&id) {
            return Ok(GroupMigrateReply {
                adopted: true,
                error: None,
            });
        }

        craft_actor::rebalance_log::line(format!(
            "node={} adopt migrated group={id} from {}",
            self.node_id.0, request.from.0
        ));

        let voters = self.voters_for_group(id, live_nodes);
        let (service, handle) = spawn_raft_group_from_bundle(
            self.node_id,
            &voters,
            id,
            self.raft.clone(),
            self.runtime.clone(),
            Arc::clone(&self.transport),
            self.forward_timeout,
            self.data_dir.as_deref(),
            &request.bundle,
        )?;
        self.sharded.insert_group(id, service);
        handles.insert(id, handle);
        Ok(GroupMigrateReply {
            adopted: true,
            error: None,
        })
    }

    /// Emit a telemetry event summarizing a rebalance pass.
    pub fn emit_rebalance(events: &craft_dashboard::EventBus, report: &GroupRebalanceReport) {
        if report.plan.adopt.is_empty() && report.plan.retire.is_empty() {
            return;
        }
        events.emit(CraftEvent::RaftGroupsRebalanced {
            adopt: report.plan.adopt.iter().map(|g| g.0).collect(),
            retire: report.plan.retire.iter().map(|g| g.0).collect(),
        });
    }
}

/// Port wired into the node router for inbound group migrations.
pub(crate) trait GroupMigratePort: Send + Sync {
    /// Apply an inbound [`GroupMigrateRequest`].
    fn handle_group_migrate(
        &self,
        request: GroupMigrateRequest,
    ) -> craft_net::transport::BoxFuture<'static, GroupMigrateReply>;
}

/// Adapter so [`MultiRaftState`] can be type-erased in the node router.
pub(crate) struct ArcGroupMigrate<M: StateMachine + Default + 'static>(pub Arc<MultiRaftState<M>>);

impl<M: StateMachine + Default + 'static> GroupMigratePort for ArcGroupMigrate<M> {
    fn handle_group_migrate(
        &self,
        request: GroupMigrateRequest,
    ) -> craft_net::transport::BoxFuture<'static, GroupMigrateReply> {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            let live_nodes = {
                let h0 = state.handles.lock().unwrap().get(&0).cloned();
                if let Some(h0) = h0 {
                    h0.status().await.map(|s| s.voters).unwrap_or_default()
                } else {
                    Vec::new()
                }
            };
            match state.adopt_group_migrate(&request, &live_nodes) {
                Ok(reply) => reply,
                Err(e) => GroupMigrateReply {
                    adopted: false,
                    error: Some(e.to_string()),
                },
            }
        })
    }
}
