//! `trembita-dashboard` — admin HTTP endpoints and the live observability
//! dashboard (health-admin-port + observability, backlog Track H).
//!
//! Everything here rides the **admin port** (default `0.0.0.0:8080/tcp`),
//! separate from the mTLS QUIC trembita wire, so ordinary probes and Prometheus
//! scrapers work without client certs. The surface is entirely read-only.
//!
//! Pieces:
//!
//! * [`Metrics`] — a small always-on Prometheus registry (`GET /metrics`).
//! * [`MetricsSink`] — optional push export port for the same samples.
//! * [`EventBus`] / [`TrembitaEvent`] — the BEAM-`:telemetry`-style event stream,
//!   consumed by user sinks and the dashboard's SSE feed.
//! * [`Observer`] + view types — the port the runtime implements to expose
//!   readiness and introspection snapshots as JSON.
//! * [`AdminServer`] — the hyper HTTP/1.1 server tying it together: `/health`,
//!   `/ready`, `/metrics`, `/introspect/*`, `/dashboard`, `/dashboard/events`.

pub use {trembita_net, trembita_runtime};

mod admin_tls;
mod dashboard;
mod metrics;
mod metrics_sink;
mod server;
mod telemetry;
mod views;

pub use admin_tls::{AdminTlsError, AdminTlsPaths, server_config as admin_tls_config};
pub use metrics::Metrics;
pub use metrics_sink::{
    MetricsSink, MultiMetricsSink, NoopMetricsSink, RecordedMetric, RecordingMetricsSink,
};
pub use server::AdminServer;
pub use telemetry::{EventBus, EventSubscription, StopReason, TraceOpts, TrembitaEvent};
pub use trembita_runtime::init_tracing;
pub use views::{
    ActorView, BoxFuture, ClusterView, NodeSummary, NodeView, Observer, QueueStreamView,
    QueuesView, RaftGroupSummary, RaftGroupsView, Readiness, SagaRecordView,
};
