//! Shared fixtures and harness helpers for crafty integration tests.
//!
//! Import the reference KV state machine and cluster polling helpers instead
//! of copying them into every `tests/` module.
#![allow(missing_docs)] // publish = false — not shipped on crates.io

pub mod actor;
pub mod clock;
#[cfg(feature = "facade")]
pub mod facade;
pub mod harness;
pub mod kv;
pub mod shard;

pub use actor::{await_node_leader, wait_for_all_node_leaders, wait_for_node_leader};
pub use clock::{
    POLL_STEP, advance, eventually, eventually_async, eventually_async_default, eventually_default,
};
#[cfg(feature = "facade")]
pub use facade::{
    await_crafty_leader, boot_local_app, wait_for_crafty_leader, wait_for_crafty_stopped,
    wait_for_each_group_cluster_leader, wait_for_group_leader_on_any, wait_for_group_leaders,
};
pub use harness::{
    TICK_PERIOD, fast_raft_config, fast_raft_config_with_seed, free_udp, test_setup,
};
pub use kv::{Cmd, Kv, KvCommand, KvError, KvMachine, KvQuery, KvResponse, Qry, Resp, TrackedKv};
pub use shard::{
    find_keys_for_two_groups, find_keys_for_two_groups_modulus,
    find_keys_for_two_groups_with_routing,
};

/// Pretty assertion macros for integration tests (colored diffs on failure).
pub use pretty_assertions::{assert_eq, assert_ne, assert_str_eq};

/// Install the workspace `tracing` subscriber (respects `RUST_LOG` / `CRAFTY_LOG`).
pub fn init_tracing() {
    crafty_actor::init_tracing();
}
