//! # trembita
//!
//! Distributed Raft + actor framework — product apps via [`TrembitaApp`], cluster APIs via [`cluster`].
//!
//! ## Product path
//!
//! ```no_run
//! use std::time::Duration;
//! use trembita::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     init_tracing();
//!     TrembitaApp::builder()
//!         .data_dir("/var/lib/trembita")
//!         .queue([QueueOpts::new("jobs", Duration::from_secs(300))])
//!         .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).with_jobs_api(true))
//!         .run(RunOpts::default().with_wait_queue("jobs"))
//!         .await
//! }
//! ```
//!
//! ## Cluster APIs
//!
//! Custom state machines and low-level control: [`cluster`] (`TrembitaCluster`, `TrembitaClusterBuilder`, queues, journals).
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
mod shutdown_signal;
mod topic_opts;
mod two_phase;
mod worker_opts;
mod workflow;
mod workflow_opts;
mod workload;

/// Cluster builder, runtime handle, queues, journals.
pub mod cluster;
/// `TREMBITA_*` boot configuration.
pub mod env;
/// Typical product imports (`TrembitaApp`, opts structs, `consumer!`, …).
pub mod prelude;
/// Rolling self-update coordinator (reference [`UpgradeMachine`](core::UpgradeMachine)).
pub mod upgrade;

#[doc(inline)]
pub use trembita_proto::{self as proto, NodeId, PROTOCOL_VERSION, Term};

#[doc(inline)]
pub use {
    trembita_actor_store as actor_store, trembita_client as client, trembita_core as core,
    trembita_events as events, trembita_jobs as jobs, trembita_runtime as runtime,
};

#[doc(inline)]
pub use {trembita_dashboard as dashboard, trembita_macros as macros, trembita_net as net};

#[doc(inline)]
pub use trembita_storage as storage;

// --- Product facade (also available via `prelude`) ---------------------------

pub use actor_group::ActorGroupOpts;
pub use app::{ShutdownOpts, TrembitaApp, TrembitaAppBuilder, journal_workflow};
pub use app_opts::RunOpts;
pub use builder::StartError;
pub use configure::TrembitaConfigure;
pub use consumer::{ConsumerGroup, ConsumerOpts, IdempotencyKeyFn, IdempotencyOpts, JobConsumer};
pub use cron_opts::CronOpts;
pub use gateway::{
    ConnectionGuard, ConnectionTracker, DEFAULT_GATEWAY_DRAIN_TIMEOUT, ExtractedIdentity,
    GatewayBearerIdentity, GatewayHandle, GatewayIdentity, GatewayOpts, GatewayRequest,
    GatewayTlsPaths, GatewayTokenIdentity, IdentityError, IdentityTypeError, NoWorkerError,
    OpenActorSessionError, SessionHandle, SessionKey, TrembitaGatewayState, spawn_gateway,
};
pub use job_opts::JobOpts;
pub use queue_opts::QueueOpts;
pub use ready::ReadyOpts;
pub use shutdown_signal::wait_for_int_or_term;
pub use topic_opts::TopicOpts;
pub use trembita_actor_store::InMemoryStore;
pub use trembita_events::TopicContext;
pub use trembita_events::{
    EventOutboxCursor, EventOutboxDrainOpts, EventOutboxError, EventOutboxPoll, EventOutboxSource,
    InMemoryEventOutboxCursor, InMemoryEventOutboxSource, OutboxEvent, RedbEventOutboxCursor,
    run_event_outbox_drainer,
};
pub use trembita_jobs::JobContext;
pub use trembita_jobs::WorkloadOpts;
pub use trembita_jobs::{
    BacklogFeedOpts, BacklogItem, BacklogRegistry, BacklogSettleOutbox, BacklogSettleOutboxOpts,
    CompositeScheduleSource, ConsumerCount, ExternalBacklog, InMemoryBacklogSettleOutbox,
    InMemoryExternalBacklog, ScheduleError, SchedulePoll, ScheduleSource, Settlement,
    StaticScheduleSource,
};
pub use trembita_runtime::{ExternalLoad, ManualExternalLoad};
pub use worker_opts::{WorkerGroup, WorkerOpts, WorkerScale};
pub use workflow::{WorkflowBuildError, WorkflowBuilder};
pub use workflow_opts::WorkflowOpts;
pub use workload::WorkloadRuntime;

#[cfg(feature = "http-jobs")]
pub use trembita_http::{
    EmbeddedAssets, EmbeddedFile, HostRouter, IntrospectApi, IntrospectApiError, Observer,
    Precompressed, StaticSite, StaticSource, embedded_from_dir, is_local_dev_host, normalize_host,
};
pub use upgrade::upgrade_api;
pub use upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradeOpts, UpgradePhase,
    UpgradeQuery, UpgradeResponse, UpgradeRunError, UpgradeState, UpgradeView, fetch_artifact,
    plan_next_grant, report_upgrade_boot, running_app_version, spawn_upgrade_coordinator,
    spawn_upgrade_runtime, upgrade_view, verify_sha256_hex,
};

pub use trembita_macros::{consumer, consumer_json};

pub use trembita_dashboard::{
    EventBus, EventSubscription, Metrics, MetricsSink, MultiMetricsSink, NoopMetricsSink,
    RecordedMetric, RecordingMetricsSink, StopReason, TraceOpts, TrembitaEvent, init_tracing,
};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
