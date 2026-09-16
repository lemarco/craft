use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use trembita_core::StateMachine;
use trembita_proto::QueueAutoscalePolicyCommand;
use trembita_runtime::{ClusterState, NodeHandle};

pub(crate) async fn propose_queue_autoscale_policies<M: StateMachine>(
    meta: NodeHandle<M>,
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

pub(crate) fn upsert_queue_autoscale_meta(
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
