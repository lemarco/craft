//! Opt-in rebalance tracing via `CRAFTY_LOG_REBALANCE=1` or
//! `RUST_LOG=crafty::rebalance=debug`.

use crafty_core::{GroupRebalancePlan, RaftGroupId};
use crafty_proto::NodeId;

use crate::tracing_init;

/// Emit a rebalance planner/executor line when the tracing filter enables
/// target `crafty::rebalance` (see `CRAFTY_LOG_REBALANCE=1` or `RUST_LOG`).
pub fn line(msg: impl AsRef<str>) {
    if std::env::var_os("CRAFTY_LOG_REBALANCE").is_some() {
        tracing_init::init_tracing();
    }
    tracing::debug!(target: "crafty::rebalance", "{}", msg.as_ref());
}

/// Log a leader rebalance plan (live members, hosted groups, adopt/retire sets).
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

/// Log that a follower skipped rebalance planning.
pub fn skipped_follower(node_id: NodeId) {
    line(format!(
        "node={} follower — skip rebalance planning",
        node_id.0
    ));
}
