//! `crafty-sim` — deterministic, seeded simulation harness (testing-strategy).
//!
//! Runs a whole cluster of `crafty-core` [`RaftNode`](crafty_core::RaftNode)s in
//! one process over a virtual network with injectable latency, loss,
//! partitions, and crashes. The [`Cluster`] checks Raft safety invariants on
//! every step, so any failing seed replays the exact schedule that broke them.
//! This is crafty's primary bug-finder (testing-strategy, backlog Track I).

pub use {crafty_core, crafty_proto};

mod harness;
mod linearizability;
mod multi_raft;
mod rebalance;
mod rng;

pub use harness::{Cluster, Fault};
pub use linearizability::{History, Model};
pub use multi_raft::MultiRaftCluster;
pub use rebalance::RebalanceSim;
