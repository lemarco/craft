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
mod log;
mod node;
mod rng;
mod shard;
mod state_machine;

pub use node::{
    Committed, Config, MembershipError, NotLeader, Output, Persist, RaftNode, ReadId, Role,
    SnapshotState,
};
pub use shard::{
    DEFAULT_GROUP_REPLICATION_FACTOR, GroupMembershipChange, GroupRebalancePlan, RaftGroupId,
    ShardId, ShardRouter, effective_replication_factor, group_host_assignment,
    group_membership_assignment, group_voters, groups_joining_node_affects,
    groups_leaving_node_affects, place_shard, plan_group_membership_change,
    plan_group_membership_sync, plan_node_group_rebalance,
};
pub use state_machine::{Command, Query, StateMachine};
