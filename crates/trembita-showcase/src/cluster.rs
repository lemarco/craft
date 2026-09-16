//! Internal cluster builder entry for maintainer harnesses (benchmarks, soaks).

use trembita::NodeId;
use trembita::core::StateMachine;
use trembita_assembly::TrembitaClusterBuilder;

/// Same as the removed public `TrembitaCluster::builder` — workspace / integration only.
#[doc(hidden)]
#[must_use]
pub fn cluster_builder<M: StateMachine + Default>(
    node_id: NodeId,
    sm: M,
) -> TrembitaClusterBuilder<M> {
    TrembitaClusterBuilder::new(node_id, sm)
}
