//! `crafty-dashboard` — admin HTTP endpoints and the live observability
//! dashboard (health-admin-port + observability, backlog Track H).
//!
//! Everything here rides the **admin port** (default `0.0.0.0:8080/tcp`),
//! separate from the mTLS QUIC crafty wire, so ordinary probes and Prometheus
//! scrapers work without client certs. The surface is entirely read-only.
//!
//! Pieces:
//!
//! * [`Metrics`] — a small always-on Prometheus registry (`GET /metrics`).
//! * [`EventBus`] / [`CraftyEvent`] — the BEAM-`:telemetry`-style event stream,
//!   consumed by user sinks and the dashboard's SSE feed.
//! * [`Observer`] + view types — the port the runtime implements to expose
//!   readiness and introspection snapshots as JSON.
//! * [`AdminServer`] — the hyper HTTP/1.1 server tying it together: `/health`,
//!   `/ready`, `/metrics`, `/introspect/*`, `/dashboard`, `/dashboard/events`.

pub use {crafty_actor, crafty_net};

mod admin_tls;
mod dashboard;
mod metrics;
mod server;
mod telemetry;
mod views;

pub use admin_tls::{AdminTlsError, AdminTlsPaths, server_config as admin_tls_config};
pub use crafty_actor::init_tracing;
pub use metrics::Metrics;
pub use server::AdminServer;
pub use telemetry::{CraftyEvent, EventBus, EventSubscription, StopReason, TraceOpts};
pub use views::{
    ActorView, BoxFuture, ClusterView, NodeSummary, NodeView, Observer, QueueStreamView,
    QueuesView, RaftGroupSummary, RaftGroupsView, Readiness, SagaRecordView,
};
