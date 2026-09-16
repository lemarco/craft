//! Custom [`StateMachine`](crate::core::StateMachine) integration tests — not public API.
//!
//! Product tests stay in `tests/`; framework tests that need [`TrembitaClusterBuilder`](crate::builder::TrembitaClusterBuilder) live here.

#![allow(clippy::large_futures)]

pub(crate) use crate::builder::TrembitaClusterBuilder;

mod support;
pub(crate) use support::{
    await_trembita_leader, boot_local_app, gateway_ops_surfaces, spawn_cluster_ops_gateway,
    wait_for_each_group_cluster_leader, wait_for_group_leaders, wait_for_trembita_leader,
    wait_for_trembita_stopped,
};

mod actor_store_resume;
mod app_cluster_reelect;
mod auto_compaction;
mod cert_reload;
mod client_keyed;
mod facade;
mod graceful_leave;
mod multi_raft;
mod persistence;
mod queue;
mod quic;
mod saga;
mod store;
mod topic;
mod two_phase;
mod upgrade_coordinator;
