//! Node assembly: [`TrembitaClusterBuilder`], [`TrembitaCluster`], routing, journals.
//!
//! **Not published** — product apps use the `trembita` facade (`TrembitaApp`). This crate exists
//! so cluster boot stays out of the semver product surface.

pub mod builder;
pub mod certs;
pub mod cluster_handle;
pub mod connections;
pub mod coordination_profile;
pub mod discovery;
pub mod empty_state_machine;
pub mod env_config;
pub mod handler;
pub mod http_auth;
mod join_pipeline;
pub mod multi_raft;
pub mod node_id;
pub mod observer;
pub mod ready;
pub mod saga;
pub mod security;
pub mod two_phase;
pub mod upgrade;
pub mod workload;

pub use trembita_proto::NodeId;

pub use builder::{StartError, TrembitaClusterBuilder};
pub use cluster_handle::{ClusterFacts, TrembitaCluster};
pub use connections::{ConnectionGuard, ConnectionTracker, HttpInFlight, InFlightGuard};
pub use discovery::Seed;
pub use empty_state_machine::EmptyStateMachine;
pub use env_config::{
    AppConfig, EnvOverrides, ProductEnv, app_config_from_env, log_non_product_env_warnings,
    product_http_from_wire,
};
pub use ready::ReadyOpts;
pub use security::Security;

/// Default HTTP gateway connection drain when env unset (mirrors product gateway config).
pub const DEFAULT_GATEWAY_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
