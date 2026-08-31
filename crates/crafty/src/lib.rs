//! # crafty
//!
//! Distributed Raft + actor framework for product apps ([`CraftyApp`]) and advanced
//! cluster programming ([`advanced`]).
//!
//! ## Product path
//!
//! ```no_run
//! use std::time::Duration;
//! use crafty::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     init_tracing();
//!     CraftyApp::builder()
//!         .data_dir("/var/lib/crafty")
//!         .queue([QueueOpts::new("jobs", Duration::from_secs(300))])
//!         .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).with_jobs_api(true))
//!         .run(RunOpts::default().with_wait_queue("jobs"))
//!         .await
//! }
//! ```
//!
//! ## Advanced path
//!
//! [`CraftyCluster`] + [`CraftyClusterBuilder`]
//! for custom state machines, journals, and queue tuning — see [`advanced`].
//!
//! Environment variables: [`mod@env`]. Architecture: `docs/` in the repository.

mod actor_group;
mod app;
mod app_opts;
mod builder;
mod certs;
mod cluster;
mod configure;
mod consumer;
mod cron_opts;
pub mod discovery;
mod env_config;
mod gateway;
mod handler;
mod multi_raft;
mod node_id;
mod observer;
mod queue_opts;
mod ready;
mod saga;
mod security;
mod two_phase;
mod workflow;
mod workflow_opts;

/// Advanced cluster / journal / queue APIs.
pub mod advanced;
/// `CRAFTY_*` boot configuration.
pub mod env;
/// Typical product imports (`CraftyApp`, opts structs, `consumer!`, …).
pub mod prelude;
/// Rolling self-update coordinator (reference [`UpgradeMachine`](core::UpgradeMachine)).
pub mod upgrade;

#[doc(inline)]
pub use crafty_proto::{self as proto, NodeId, PROTOCOL_VERSION, Term};

#[doc(inline)]
pub use {crafty_actor as actor, crafty_client as client, crafty_core as core};

#[doc(inline)]
pub use {crafty_dashboard as dashboard, crafty_macros as macros, crafty_net as net};

#[doc(inline)]
pub use crafty_storage as storage;

// --- Product facade (also available via `prelude`) ---------------------------

pub use actor_group::ActorGroupOpts;
pub use app::{CraftyApp, CraftyAppBuilder, ShutdownOpts, journal_workflow};
pub use app_opts::RunOpts;
pub use builder::StartError;
pub use configure::CraftyConfigure;
pub use consumer::{ConsumerGroup, ConsumerOpts, JobConsumer};
pub use cron_opts::CronOpts;
pub use gateway::{
    ConnectionGuard, ConnectionTracker, CraftyGatewayState, DEFAULT_GATEWAY_DRAIN_TIMEOUT,
    ExtractedIdentity, GatewayHandle, GatewayIdentity, GatewayOpts, GatewayRequest,
    GatewayTlsPaths, GatewayTokenIdentity, IdentityError, IdentityTypeError, NoWorkerError,
    OpenActorSessionError, SessionHandle, SessionKey, spawn_gateway,
};
pub use queue_opts::QueueOpts;
pub use ready::ReadyOpts;
pub use workflow::{WorkflowBuildError, WorkflowBuilder};
pub use workflow_opts::WorkflowOpts;

pub use advanced::{CraftyCluster, CraftyClusterBuilder};

#[cfg(feature = "http-jobs")]
pub use upgrade::upgrade_api;
pub use upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradeOpts, UpgradePhase,
    UpgradeQuery, UpgradeResponse, UpgradeRunError, UpgradeState, UpgradeView, fetch_artifact,
    plan_next_grant, report_upgrade_boot, running_app_version, spawn_upgrade_coordinator,
    spawn_upgrade_runtime, upgrade_view, verify_sha256_hex,
};

pub use crafty_macros::consumer;

pub use crafty_dashboard::{
    CraftyEvent, EventBus, EventSubscription, Metrics, StopReason, TraceOpts, init_tracing,
};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
