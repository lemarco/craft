//! `craft-core` — the pure Raft consensus state machine (no I/O).
//!
//! [`RaftNode`] drives leader election and log replication deterministically:
//! it consumes events and returns [`Output`] effects for an outer runtime to
//! execute (ADR 030). Because it performs no I/O and derives all randomness
//! from a seed, an entire cluster can be simulated reproducibly (ADR 029).
//!
//! Joint-consensus membership (ADR 016), ReadIndex (ADR 005), and snapshots
//! build on this foundation in later increments.

pub use craft_proto as proto;

mod config;
mod log;
mod node;
mod rng;
mod state_machine;

pub use config::Configuration;
pub use node::{
    Committed, Config, MembershipError, NotLeader, Output, Persist, RaftNode, ReadId, Role,
    SnapshotState,
};
pub use state_machine::{Command, Query, StateMachine};
