//! Multi-Raft rebalance execution for the facade (ADR 031).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use craft_actor::{
    GroupRebalanceReport, NodeHandle, RaftGroupReconciler, RuntimeConfig, ShardedNodeService,
    spawn_raft_group,
};
use craft_core::RaftGroupId;
use craft_core::{Config, StateMachine};
use craft_dashboard::CraftEvent;
use craft_net::Transport;
use craft_proto::NodeId;
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
    pub members: Vec<NodeId>,
}

impl<M: StateMachine + Default + 'static> MultiRaftState<M> {
    /// Plan and apply local adopt/retire actions (leader-only planning).
    pub async fn rebalance(
        &self,
        facts: Arc<ClusterFacts>,
    ) -> Result<GroupRebalanceReport, StorageError> {
        let hosted = self.sharded.hosted_group_ids();
        let reconciler = RaftGroupReconciler::new(self.node_id, self.catalog.clone(), facts);
        let report = reconciler.reconcile_local(&hosted);
        if !report.ran_as_leader {
            return Ok(report);
        }

        craft_actor::rebalance_log::line(format!(
            "node={} applying adopt={:?} retire={:?}",
            self.node_id.0,
            report.plan.adopt.iter().map(|g| g.0).collect::<Vec<_>>(),
            report.plan.retire.iter().map(|g| g.0).collect::<Vec<_>>(),
        ));

        let mut handles = self.handles.lock().unwrap();

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
            craft_actor::rebalance_log::line(format!("node={} adopt group={id}", self.node_id.0));
            let (service, handle) = spawn_raft_group(
                self.node_id,
                &self.members,
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

    /// Emit a telemetry event summarizing a rebalance pass.
    pub fn emit_rebalance(events: &craft_dashboard::EventBus, report: &GroupRebalanceReport) {
        if !report.ran_as_leader {
            return;
        }
        events.emit(CraftEvent::RaftGroupsRebalanced {
            adopt: report.plan.adopt.iter().map(|g| g.0).collect(),
            retire: report.plan.retire.iter().map(|g| g.0).collect(),
        });
    }
}
