//! `crafty-core` — the pure Raft consensus state machine (no I/O).
//!
//! [`RaftNode`] drives leader election and log replication deterministically:
//! it consumes events and returns [`Output`] effects for an outer runtime to
//! execute (architecture-style). Because it performs no I/O and derives all randomness
//! from a seed, an entire cluster can be simulated reproducibly (testing-strategy).
//!
//! Joint-consensus membership (membership-early), `ReadIndex` (read-consistency), and snapshots
//! build on this foundation in later increments.

pub use crafty_proto as proto;

mod compaction;
mod config;
mod failure_detector;
pub mod kv;
pub mod upgrade;
mod log;
mod node;
mod rng;
mod shard;
mod state_machine;
mod two_phase;

pub use compaction::{
    CompactionPolicy, CompactionStats, DEFAULT_COMPACT_BYTES, DEFAULT_COMPACT_ENTRIES,
    compaction_stats, entry_estimated_bytes, should_compact,
};
pub use failure_detector::{
    AckWindowLiveness, FailureDetectorKind, PhiAccrualDetector, PhiAccrualLiveness,
    ReachabilityConfig,
};
pub use kv::{Kv, KvCommand, KvError, KvMachine, KvQuery, KvResponse};
pub use upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradePhase, UpgradeQuery,
    UpgradeResponse, UpgradeState, UpgradeStateMachine, UpgradeView, plan_next_grant,
    upgrade_state_for_planning, upgrade_view,
};
pub use node::{
    CatalogProposeError, Committed, Config, MembershipError, NotLeader, Output, Persist, RaftNode,
    ReadId, Role, SnapshotState,
};
pub use shard::{
    CatalogError, CatalogExpansionPlan, DEFAULT_GROUP_LEARNER_FACTOR,
    DEFAULT_GROUP_REPLICATION_FACTOR, GroupMembershipChange, GroupRebalancePlan,
    GroupReplicationTarget, MAX_VIRTUAL_SHARDS, META_RAFT_GROUP_ID, RaftGroupId,
    ShardCountExpansionPlan, ShardExpansionError, ShardId, ShardRouter, ShardRoutingKind,
    ShardRoutingSwitchError, ShardRoutingSwitchPlan, StableShardActivationError,
    StableShardActivationPlan, StableShardRouter, effective_replication_factor,
    group_host_assignment, group_learners, group_membership_assignment, group_voters,
    groups_joining_node_affects, groups_leaving_node_affects, is_meta_raft_group,
    node_should_host_group, place_shard, plan_catalog_expansion, plan_group_membership_change,
    plan_group_membership_sync, plan_node_group_rebalance, plan_shard_count_expansion,
    plan_stable_shard_activation, plan_switch_to_stable_routing, shard_is_active,
    stable_router_preserves_routable_keys, validate_catalog, virtual_shard_for,
};
pub use state_machine::{Command, Query, StateMachine};
pub use two_phase::{
    TWO_PHASE_DEFAULT_PREPARE_TIMEOUT_MS, TWO_PHASE_MAX_GROUPS, TWO_PHASE_MAX_PAYLOAD,
    TWO_PHASE_MAX_STEPS, TwoPhasePlan, TwoPhasePlanError, TwoPhaseStep, validate_two_phase_plan,
};
