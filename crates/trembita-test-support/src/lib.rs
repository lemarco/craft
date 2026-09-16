//! Shared fixtures and harness helpers for trembita integration tests.
//!
//! Import the reference KV state machine and cluster polling helpers instead
//! of copying them into every `tests/` module.
#![allow(missing_docs)]

pub mod clock;
pub mod harness;
pub mod kv;
pub mod shard;

pub use clock::{
    POLL_STEP, advance, eventually, eventually_async, eventually_async_default, eventually_default,
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
