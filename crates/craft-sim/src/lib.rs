//! `craft-sim` — deterministic, seeded simulation harness (testing-strategy).
//!
//! Runs a whole cluster of `craft-core` [`RaftNode`](craft_core::RaftNode)s in
//! one process over a virtual network with injectable latency, loss,
//! partitions, and crashes. The [`Cluster`] checks Raft safety invariants on
//! every step, so any failing seed replays the exact schedule that broke them.
//! This is craft's primary bug-finder (testing-strategy, backlog Track I).

pub use {craft_core, craft_proto};

mod harness;
mod linearizability;
mod multi_raft;
mod rebalance;
mod rng;

pub use harness::{Cluster, Fault};
pub use linearizability::{History, Model};
pub use multi_raft::MultiRaftCluster;
pub use rebalance::RebalanceSim;
