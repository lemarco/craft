//! Opt-in rebalance tracing via `CRAFT_LOG_REBALANCE=1` or
//! `RUST_LOG=craft::rebalance=debug`.

use craft_core::{GroupRebalancePlan, RaftGroupId};
use craft_proto::NodeId;

use crate::tracing_init;

/// Emit a rebalance planner/executor line when the tracing filter enables
/// target `craft::rebalance` (see `CRAFT_LOG_REBALANCE=1` or `RUST_LOG`).
pub fn line(msg: impl AsRef<str>) {
    if std::env::var_os("CRAFT_LOG_REBALANCE").is_some() {
        tracing_init::init_tracing();
    }
    tracing::debug!(target: "craft::rebalance", "{}", msg.as_ref());
}

pub fn plan(node_id: NodeId, live: &[NodeId], hosted: &[RaftGroupId], plan: &GroupRebalancePlan) {
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
