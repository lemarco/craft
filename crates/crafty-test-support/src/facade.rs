//! Helpers for [`crafty::CraftyCluster`] and [`crafty::CraftyApp`] integration tests.

use std::sync::Arc;

use crafty::CraftyApp;
use crafty::CraftyAppBuilder;
use crafty::ReadyOpts;
use crafty::RunOpts;
use crafty::advanced::CraftyCluster;
use crafty::core::{Role, StateMachine};

use crate::clock::{POLL_STEP, advance};

/// Boot a local [`CraftyApp`] for integration tests (no Ctrl-C loop).
///
/// # Panics
/// When boot fails.
pub async fn boot_local_app(
    builder: CraftyAppBuilder,
    wait_ready: Option<ReadyOpts>,
) -> Arc<CraftyApp> {
    let mut opts = RunOpts::local();
    opts.wait_ready = wait_ready;
    builder.boot_for_test(opts).await.expect("boot_local_app")
}

/// Poll until one cluster in `clusters` reports leader, or panic after ~10s.
///
/// # Panics
/// If no cluster elects a leader within the poll budget.
pub async fn await_crafty_leader<M>(clusters: &[Arc<CraftyCluster<M>>]) -> Arc<CraftyCluster<M>>
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
///
/// # Panics
/// If `cluster` fails to elect a leader within the poll budget.
pub async fn wait_for_crafty_leader<M>(cluster: &CraftyCluster<M>)
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
///
/// # Panics
/// If not every hosted group elects a leader within the poll budget.
pub async fn wait_for_group_leaders<M>(cluster: &CraftyCluster<M>)
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
///
/// # Panics
/// If any group lacks a leader across the cluster within the poll budget.
pub async fn wait_for_each_group_cluster_leader<M>(
    clusters: &[Arc<CraftyCluster<M>>],
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
///
/// # Panics
/// If no cluster elects a leader for `group` within the poll budget.
pub async fn wait_for_group_leader_on_any<M>(
    clusters: &[Arc<CraftyCluster<M>>],
    group: u32,
    extra: &[&CraftyCluster<M>],
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

async fn is_group_leader<M>(cluster: &CraftyCluster<M>, group: u32) -> bool
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
pub async fn wait_for_crafty_stopped<M>(cluster: &CraftyCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    cluster.shutdown_and_wait().await;
}
