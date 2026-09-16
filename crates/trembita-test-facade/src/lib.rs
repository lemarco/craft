//! `TrembitaApp` and gateway helpers for facade integration tests (not published).

#![allow(missing_docs)]

pub mod capability;
pub mod facade;
pub mod gateway;

pub use capability::{boot_local_app_with_capabilities, manifest_with_capabilities};
pub use facade::{
    await_trembita_leader, boot_local_app, boot_local_app_with_consumers,
    wait_for_each_group_cluster_leader, wait_for_group_leader_on_any, wait_for_group_leaders,
    wait_for_trembita_app_leader, wait_for_trembita_leader, wait_for_trembita_stopped,
};
pub use gateway::{
    gateway_actors_surfaces, gateway_introspect_config_identity, gateway_introspect_surfaces,
    gateway_introspect_surfaces_identity, gateway_jobs_config, gateway_jobs_config_identity,
    gateway_jobs_surfaces, gateway_jobs_surfaces_identity, gateway_ops_config,
    gateway_ops_surfaces, gateway_workflows_config, gateway_workflows_surfaces,
    spawn_cluster_ops_gateway, spawn_test_gateway,
};
