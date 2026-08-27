//! Helpers for [`craft::CraftCluster`] integration tests.

use std::sync::Arc;

use craft::CraftCluster;
use craft::core::{Role, StateMachine};

use crate::clock::{POLL_STEP, advance};

/// Poll until one cluster in `clusters` reports leader, or panic after ~10s.
pub async fn await_craft_leader<M>(clusters: &[Arc<CraftCluster<M>>]) -> Arc<CraftCluster<M>>
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..1000 {
        for c in clusters {
            if c.is_leader().await {
                return Arc::clone(c);
            }
        }
        advance(POLL_STEP).await;
    }
    panic!("no leader elected");
}

/// Poll until `cluster` reports leader, or panic after ~5s.
pub async fn wait_for_craft_leader<M>(cluster: &CraftCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        if cluster.is_leader().await {
            return;
        }
        advance(POLL_STEP).await;
    }
    panic!("cluster failed to elect a leader");
}

/// Poll until every Raft group on `cluster` has a local leader, or panic.
pub async fn wait_for_group_leaders<M>(cluster: &CraftCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        let mut leaders = 0usize;
        for handle in cluster.group_handles() {
            if let Some(status) = handle.status().await
                && status.role == Role::Leader
            {
                leaders += 1;
            }
        }
        if leaders == cluster.raft_groups() as usize {
            return;
        }
        advance(POLL_STEP).await;
    }
    panic!("not all raft groups elected a leader");
}

/// Poll until each group index has a leader on at least one cluster, or panic.
pub async fn wait_for_each_group_cluster_leader<M>(
    clusters: &[Arc<CraftCluster<M>>],
    group_count: u32,
) where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..1000 {
        let mut ready = true;
        'groups: for g in 0..group_count {
            for c in clusters {
                let Some(handle) = c.group_handles().get(g as usize) else {
                    continue;
                };
                if let Some(status) = handle.status().await
                    && status.role == Role::Leader
                {
                    continue 'groups;
                }
            }
            ready = false;
            break;
        }
        if ready {
            return;
        }
        advance(POLL_STEP).await;
    }
    panic!("not all raft groups elected a leader across the cluster");
}

/// Poll until at least one cluster (plus optional non-`Arc` peers) is leader for
/// `group`, or panic after ~10s.
pub async fn wait_for_group_leader_on_any<M>(
    clusters: &[Arc<CraftCluster<M>>],
    group: u32,
    extra: &[&CraftCluster<M>],
) where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..1000 {
        for cluster in clusters {
            if is_group_leader(cluster.as_ref(), group).await {
                return;
            }
        }
        for cluster in extra {
            if is_group_leader(cluster, group).await {
                return;
            }
        }
        advance(POLL_STEP).await;
    }
    panic!("no leader elected for group {group}");
}

async fn is_group_leader<M>(cluster: &CraftCluster<M>, group: u32) -> bool
where
    M: StateMachine + Send + Sync + 'static,
{
    let Some(handle) = cluster.group_handle(group) else {
        return false;
    };
    handle
        .status()
        .await
        .is_some_and(|status| status.role == Role::Leader)
}

/// Stop `cluster` and wait until every consensus runtime has exited so redb
/// files can be reopened.
pub async fn wait_for_craft_stopped<M>(cluster: &CraftCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    cluster.shutdown_and_wait().await;
}
