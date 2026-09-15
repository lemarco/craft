//! Multi-Raft **control plane**: catalog, membership, rebalance, supervisor.
//!
//! Hosts cluster-wide reconciliation and group placement. Per-group Raft drivers
//! and actors live in [`crate::data_plane`].

pub use crate::group_membership::{GroupMembershipSyncReport, sync_hosted_group_membership};
pub use crate::group_rebalance::{GroupRebalanceReport, RaftGroupReconciler};
pub use crate::meta::{MetaCommand, MetaError, MetaQuery, MetaResponse, MetaStateMachine};
pub use crate::placement::{
    ClusterControl, ClusterScaleError, MigrateError, NOT_LEADER_REASON, RemoteSpawnError,
    ScalePlan, plan_scale,
};
pub use crate::rebalance_log;
pub use crate::sharded::{
    MultiRaftSpawnResult, ShardedNodeService, spawn_multi_raft_node, spawn_raft_group,
    spawn_raft_group_from_bundle,
};
pub use crate::supervisor::{ClusterState, ClusterSupervisor, GroupReconcile, ReconcileReport};
