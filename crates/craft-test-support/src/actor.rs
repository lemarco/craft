//! Helpers for [`craft_actor::NodeHandle`] integration tests.

use craft_actor::NodeHandle;
use craft_actor::craft_core::{Role, StateMachine};
use craft_actor::craft_proto::NodeId;

use crate::harness::TICK_PERIOD;

/// Poll node statuses until one reports `Leader`, or panic after ~5s.
pub async fn await_node_leader<M>(handles: &[(NodeId, NodeHandle<M>)]) -> NodeId
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        for (id, handle) in handles {
            if let Some(status) = handle.status().await
                && status.role == Role::Leader
            {
                return *id;
            }
        }
        tokio::time::sleep(TICK_PERIOD).await;
    }
    panic!("no leader elected");
}

/// Poll until every handle reports leader, or panic after ~5s.
pub async fn wait_for_all_node_leaders<M>(handles: &[NodeHandle<M>])
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        let mut leaders = 0usize;
        for handle in handles {
            if let Some(status) = handle.status().await
                && status.role == Role::Leader
            {
                leaders += 1;
            }
        }
        if leaders == handles.len() {
            return;
        }
        tokio::time::sleep(TICK_PERIOD).await;
    }
    panic!("not all raft groups elected a leader");
}

/// Poll until `handle` reports leader, or panic after ~5s.
pub async fn wait_for_node_leader<M>(handle: &NodeHandle<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        if let Some(status) = handle.status().await
            && status.role == Role::Leader
        {
            return;
        }
        tokio::time::sleep(TICK_PERIOD).await;
    }
    panic!("node failed to elect a leader");
}
