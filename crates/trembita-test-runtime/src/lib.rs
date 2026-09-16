//! [`trembita_runtime::NodeHandle`] test helpers (not published).

#![allow(missing_docs)]

pub mod actor;

pub use actor::{await_node_leader, wait_for_all_node_leaders, wait_for_node_leader};

/// Install the workspace `tracing` subscriber (respects `RUST_LOG` / `TREMBITA_LOG`).
pub fn init_tracing() {
    trembita_runtime::init_tracing();
}
