//! # trembita
//!
//! Distributed Raft + actor framework — product apps via [`TrembitaApp`], cluster APIs via [`cluster`].
//!
//! ## Product path
//!
//! ```no_run
//! use trembita::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     init_tracing();
//!     TrembitaApp::from_env()?
//!         .jobs([JobOpts::new("jobs").lease(std::time::Duration::from_secs(300))])
//!         .run()
//!         .await
//! }
//! ```
//!
//! ## Dependencies and features
//!
//! Add a single crate to `Cargo.toml` and enable integrations via features:
//!
//! ```toml
//! trembita = { version = "0.3", features = ["http-jobs", "dev-certs", "external-backlog"] }
//! ```
//!
//! | Feature | Purpose |
//! |---------|---------|
//! | `http-jobs` (default) | Product HTTP gateway (`Gateway`, `/jobs/*`, …) |
//! | `dev-certs` | Ephemeral mTLS for local development |
//! | `redis-store` | Redis [`ActorStateStore`](trembita_actor_store::ActorStateStore) |
//! | `external-backlog` | Postgres [`ExternalBacklog`] adapter |
//! | `domain-outbox` | Postgres [`EventOutboxSource`] adapter |
//!
//! Full reference: [facade ADR](https://gitlab.com/lemarco/trembita/-/blob/main/docs/decisions/facade.md).
//!
//! ## Cluster APIs
//!
//! Runtime embedding (handles, queues, journals): [`cluster`] (`TrembitaCluster`, …).
//!
//! Environment variables: [`mod@env`]. Architecture: `docs/` in the repository.

mod actor_group;
mod app;
mod app_opts;
mod builder;
mod capability;
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
mod scheduled_workflow_opts;
mod security;
mod shutdown_signal;
mod topic_opts;
mod two_phase;
mod work_trigger;
mod worker_opts;
mod workflow;
mod workflow_opts;
mod workload;

#[cfg(test)]
mod integration;

/// Maintainer showcases (custom SM) — not product API.
#[doc(hidden)]
pub mod workspace_showcase;

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
pub use app::{
    AppManifest, DefaultGatewayApis, JobsPreset, RealtimePreset, ScheduleSourceOpts, ShutdownOpts,
    TestBoot, TopicsPreset, TrembitaApp, TrembitaAppBuilder, journal_workflow,
};
pub use app_opts::RunOpts;
pub use builder::StartError;
pub use capability::{
    CallBuilder, CapCallOpts, CapEnqueueOutcome, CapError, CapGroup, CapGroupScale, CapManifest,
    CapOp, CapQueued, CapRequest, CapRuntime, CapVia, CapWire, OpCtx, Route, deliver_event,
    deliver_queued, enqueue, fire, invoke, publish_event,
};
pub use configure::TrembitaConfigure;
pub use consumer::{ConsumerGroup, ConsumerOpts, IdempotencyKeyFn, IdempotencyOpts, JobConsumer};
pub use cron_opts::CronOpts;
#[cfg(feature = "http-jobs")]
pub use gateway::{
    CapEnqueueHandler, CapFireHandler, CapInvokeHandler, ProductRoutes, cap_enqueue, cap_fire,
    cap_invoke, cluster_ops_route_table, spawn_cluster_ops_http,
};
pub use gateway::{
    ConnectionGuard, ConnectionTracker, DEFAULT_GATEWAY_DRAIN_TIMEOUT, ExtractedIdentity,
    GatewayBearerIdentity, GatewayConfig, GatewayHandle, GatewayIdentity, GatewayOpts,
    GatewayRequest, GatewayTlsPaths, GatewayTokenIdentity, IdentityError, IdentityTypeError,
    NoWorkerError, OpenActorSessionError, SessionHandle, SessionKey, TrembitaGatewayState,
    WrappedGatewayService, build_gateway_service, spawn_gateway,
};
pub use job_opts::JobOpts;
pub use queue_opts::QueueOpts;
pub use ready::ReadyOpts;
pub use scheduled_workflow_opts::ScheduledWorkflowOpts;
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
    CachedDepth, CompositeScheduleSource, ConsumerCount, DepthCache, ExternalBacklog,
    InMemoryBacklogSettleOutbox, InMemoryExternalBacklog, ScheduleError, SchedulePoll,
    ScheduleSource, Settlement, StaticScheduleSource, WorkTrigger, WorkTriggerError,
};
pub use trembita_runtime::{ExternalLoad, ManualExternalLoad};
pub use work_trigger::{DispatchError, DispatchOutcome, dispatch_work_trigger};
pub use worker_opts::{WorkerGroup, WorkerOpts, WorkerScale};
pub use workflow::{WorkflowBuildError, WorkflowBuilder};
pub use workflow_opts::WorkflowOpts;
pub use workload::WorkloadRuntime;

#[cfg(feature = "http-jobs")]
pub use gateway::ws::{
    RawWs, StickyWs, WsBroadcastHub, WsMessage, WsMount, WsNotifyHub, WsSubscribeCmd,
    apply_ws_mounts, futures_util, mount_raw_websocket, mount_sticky_websocket,
    mount_sticky_websocket_group, run_sticky_cast_loop, run_sticky_text_loop, run_text_loop,
    server_stream, tokio_tungstenite,
};

#[cfg(feature = "http-jobs")]
pub use trembita_http::{
    AuthMode, CookieConfig, CorsPolicy, EmbeddedAssets, EmbeddedFile, Gateway, GatewayBuildError,
    GatewayService, HttpError, IntrospectApi, IntrospectApiError, Observer, OpsApi, Precompressed,
    RequestCtx, Response, ResponseBody, RouteDescriptor, RouteTable, RouteTableDiff, SessionGate,
    StaticSite, StaticSource, Surface, UpgradeStream, accept_websocket, embedded_from_dir,
    is_local_dev_host, normalize_host, routing_to_http_response,
};

#[cfg(feature = "redis-store")]
#[doc(inline)]
pub use trembita_store_redis as store_redis;

#[cfg(feature = "redis-store")]
pub use trembita_store_redis::{RedisStore, RedisTlsConfig};

#[cfg(feature = "external-backlog")]
#[doc(inline)]
pub use trembita_backlog_postgres as backlog_postgres;

#[cfg(feature = "external-backlog")]
pub use trembita_backlog_postgres::{PgBacklog, PgBacklogSchema, SharedPgBacklog};

#[cfg(feature = "domain-outbox")]
#[doc(inline)]
pub use trembita_events_postgres as events_postgres;

#[cfg(feature = "domain-outbox")]
pub use trembita_events_postgres::{PgEventOutboxSchema, PgEventOutboxSource};
pub use upgrade::upgrade_api;
pub use upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradeOpts, UpgradePhase,
    UpgradeQuery, UpgradeResponse, UpgradeRunError, UpgradeState, UpgradeView, fetch_artifact,
    plan_next_grant, report_upgrade_boot, running_app_version, spawn_upgrade_coordinator,
    spawn_upgrade_runtime, upgrade_view, verify_sha256_hex,
};

pub use trembita_macros::{cap_handler, cap_request, consumer, consumer_json};

pub use trembita_dashboard::{
    EventBus, EventSubscription, Metrics, MetricsSink, MultiMetricsSink, NoopMetricsSink,
    RecordedMetric, RecordingMetricsSink, StopReason, TraceOpts, TrembitaEvent, init_tracing,
};

#[cfg(feature = "otlp-metrics")]
pub use trembita_runtime::{
    MetricsOpts, TracingOpts, init_metrics_with_otlp, init_tracing_with_otlp,
};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Register a job stream with [`JobOpts::product`] defaults.
#[macro_export]
macro_rules! product_job {
    ($stream:expr, $consumer:expr) => {
        $crate::JobOpts::product($stream, $consumer)
    };
}

/// Chain [`CapGroup`](crate::CapGroup) `{handler}_register` helpers from [`cap_handler`](crate::cap_handler).
#[macro_export]
macro_rules! cap_register_chain {
    ($init:expr $(, $register:ident)* $(,)?) => {{
        let mut __cap_group = $init;
        $( __cap_group = $register(__cap_group); )*
        __cap_group
    }};
}
