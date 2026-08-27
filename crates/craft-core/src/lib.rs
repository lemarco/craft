//! `craft-core` — the pure Raft consensus state machine (no I/O).
//!
//! [`RaftNode`] drives leader election and log replication deterministically:
//! it consumes events and returns [`Output`] effects for an outer runtime to
//! execute (architecture-style). Because it performs no I/O and derives all randomness
//! from a seed, an entire cluster can be simulated reproducibly (testing-strategy).
//!
//! Joint-consensus membership (membership-early), ReadIndex (read-consistency), and snapshots
//! build on this foundation in later increments.

pub use craft_proto as proto;

mod config;
mod failure_detector;
mod log;
mod node;
mod rng;
mod shard;
mod state_machine;

pub use failure_detector::{
    AckWindowLiveness, FailureDetectorKind, PhiAccrualDetector, PhiAccrualLiveness,
    ReachabilityConfig,
};
pub use node::{
    CatalogProposeError, Committed, Config, MembershipError, NotLeader, Output, Persist, RaftNode,
    ReadId, Role, SnapshotState,
};
pub use shard::{
    CatalogError, CatalogExpansionPlan, DEFAULT_GROUP_LEARNER_FACTOR,
    DEFAULT_GROUP_REPLICATION_FACTOR, GroupMembershipChange, GroupRebalancePlan,
    GroupReplicationTarget, MAX_VIRTUAL_SHARDS, RaftGroupId, ShardCountExpansionPlan,
    ShardExpansionError, ShardId, ShardRouter, StableShardActivationError,
    StableShardActivationPlan, StableShardRouter, effective_replication_factor,
    group_host_assignment, group_learners, group_membership_assignment, group_voters,
    groups_joining_node_affects, groups_leaving_node_affects, place_shard, plan_catalog_expansion,
    plan_group_membership_change, plan_group_membership_sync, plan_node_group_rebalance,
    plan_shard_count_expansion, plan_stable_shard_activation, shard_is_active,
    stable_router_preserves_routable_keys, validate_catalog, virtual_shard_for,
};
pub use state_machine::{Command, Query, StateMachine};
