//! # crafty
//!
//! Distributed Raft + actor framework — product apps via [`CraftyApp`], cluster APIs via [`cluster`].
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
//! ## Cluster APIs
//!
//! Custom state machines and low-level control: [`cluster`] (`CraftyCluster`, `CraftyClusterBuilder`, queues, journals).
//!
//! Environment variables: [`mod@env`]. Architecture: `docs/` in the repository.

mod actor_group;
mod app;
mod app_opts;
mod builder;
mod certs;
mod cluster_handle;
mod configure;
mod consumer;
mod cron_opts;
pub mod discovery;
mod env_config;
mod gateway;
mod handler;
mod job_opts;
mod multi_raft;
mod node_id;
mod observer;
mod queue_opts;
mod ready;
mod saga;
mod security;
mod topic_opts;
mod two_phase;
mod worker_opts;
mod workflow;
mod workflow_opts;
mod workload;

/// Cluster builder, runtime handle, queues, journals.
pub mod cluster;
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
pub use consumer::{ConsumerGroup, ConsumerOpts, IdempotencyKeyFn, IdempotencyOpts, JobConsumer};
pub use crafty_actor::InMemoryStore;
pub use crafty_actor::JobContext;
pub use crafty_actor::TopicContext;
pub use crafty_actor::WorkloadOpts;
pub use crafty_actor::{
    BacklogFeedOpts, BacklogItem, BacklogRegistry, BacklogSettleOutbox, BacklogSettleOutboxOpts,
    CompositeScheduleSource, ConsumerCount, ExternalBacklog, ExternalLoad,
    InMemoryBacklogSettleOutbox, InMemoryExternalBacklog, ManualExternalLoad, ScheduleError, SchedulePoll, ScheduleSource, Settlement,
    StaticScheduleSource,
};
pub use cron_opts::CronOpts;
pub use gateway::{
    ConnectionGuard, ConnectionTracker, CraftyGatewayState, DEFAULT_GATEWAY_DRAIN_TIMEOUT,
    ExtractedIdentity, GatewayBearerIdentity, GatewayHandle, GatewayIdentity, GatewayOpts,
    GatewayRequest, GatewayTlsPaths, GatewayTokenIdentity, IdentityError, IdentityTypeError,
    NoWorkerError, OpenActorSessionError, SessionHandle, SessionKey, spawn_gateway,
};
pub use job_opts::JobOpts;
pub use queue_opts::QueueOpts;
pub use ready::ReadyOpts;
pub use topic_opts::TopicOpts;
pub use worker_opts::{WorkerGroup, WorkerOpts, WorkerScale};
pub use workflow::{WorkflowBuildError, WorkflowBuilder};
pub use workflow_opts::WorkflowOpts;
pub use workload::WorkloadRuntime;

#[cfg(feature = "http-jobs")]
pub use crafty_http::{HostRouter, is_local_dev_host, normalize_host};
pub use upgrade::upgrade_api;
pub use upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradeOpts, UpgradePhase,
    UpgradeQuery, UpgradeResponse, UpgradeRunError, UpgradeState, UpgradeView, fetch_artifact,
    plan_next_grant, report_upgrade_boot, running_app_version, spawn_upgrade_coordinator,
    spawn_upgrade_runtime, upgrade_view, verify_sha256_hex,
};

pub use crafty_macros::{consumer, consumer_json};

pub use crafty_dashboard::{
    CraftyEvent, EventBus, EventSubscription, Metrics, StopReason, TraceOpts, init_tracing,
};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
