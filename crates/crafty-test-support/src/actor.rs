//! Helpers for [`crafty_actor::NodeHandle`] integration tests.

use crafty_actor::NodeHandle;
use crafty_actor::crafty_core::{Role, StateMachine};
use crafty_actor::crafty_proto::NodeId;

use crate::clock::{POLL_STEP, advance};

/// Poll node statuses until one reports `Leader`, or panic after ~5s.
///
/// # Panics
/// If no node elects a leader within the poll budget.
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
        advance(POLL_STEP).await;
    }
    panic!("no leader elected");
}

/// Poll until every handle reports leader, or panic after ~5s.
///
/// # Panics
/// If not every handle elects a leader within the poll budget.
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
        advance(POLL_STEP).await;
    }
    panic!("not all raft groups elected a leader");
}

/// Poll until `handle` reports leader, or panic after ~5s.
///
/// # Panics
/// If the node fails to elect a leader within the poll budget.
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
        advance(POLL_STEP).await;
    }
    panic!("node failed to elect a leader");
}
