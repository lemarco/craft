//! Opt-in rebalance tracing via `CRAFT_LOG_REBALANCE=1`.

use craft_core::{GroupRebalancePlan, RaftGroupId};
use craft_proto::NodeId;

/// When `CRAFT_LOG_REBALANCE` is set, write a line to stderr (visible in tests/CI logs).
pub(crate) fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("CRAFT_LOG_REBALANCE").is_some())
}

pub fn line(msg: impl AsRef<str>) {
    if enabled() {
        eprintln!("[craft:rebalance] {}", msg.as_ref());
    }
}

pub fn plan(node_id: NodeId, live: &[NodeId], hosted: &[RaftGroupId], plan: &GroupRebalancePlan) {
    if !enabled() {
        return;
    }
    line(format!(
        "node={} leader plan live={:?} hosted={:?} adopt={:?} retire={:?}",
        node_id.0,
        live.iter().map(|n| n.0).collect::<Vec<_>>(),
        hosted.iter().map(|g| g.0).collect::<Vec<_>>(),
        plan.adopt.iter().map(|g| g.0).collect::<Vec<_>>(),
        plan.retire.iter().map(|g| g.0).collect::<Vec<_>>(),
    ));
}

pub fn skipped_follower(node_id: NodeId) {
    line(format!(
        "node={} follower — skip rebalance planning",
        node_id.0
    ));
}
