//! # crafty
//!
//! A library-first distributed framework: write your state machine and actors
//! once, then run the same binary on as many nodes as you like. Nodes form a
//! Raft cluster over HTTP/3 (mTLS), replicate a linearizable state machine, and
//! host supervised actors that can message, spawn, and migrate across nodes.
//!
//! This facade re-exports the stable public API; most users depend only on
//! `crafty` (library-and-publishing). The [`CraftyCluster`] builder is the main entry point:
//!
//! ```no_run
//! use std::time::Duration;
//! use crafty::{CraftyCluster, NodeId};
//! use crafty::net::LocalNetwork;
//! # use crafty::core::{Config, StateMachine};
//! # use crafty::proto::LogIndex;
//! # #[derive(Default)]
//! # struct Counter(u64);
//! # impl StateMachine for Counter {
//! #     type Command = u64; type Query = (); type Response = u64; type Error = std::convert::Infallible;
//! #     fn apply(&mut self, _: LogIndex, c: &u64) -> Result<u64, Self::Error> { self.0 += *c; Ok(self.0) }
//! #     fn query(&self, _: &()) -> Result<u64, Self::Error> { Ok(self.0) }
//! #     fn snapshot(&self) -> Result<Vec<u8>, Self::Error> { Ok(self.0.to_le_bytes().to_vec()) }
//! #     fn restore(&mut self, b: &[u8]) -> Result<(), Self::Error> { self.0 = u64::from_le_bytes(b.try_into().unwrap()); Ok(()) }
//! # }
//! # async fn run() {
//! let net = LocalNetwork::new();
//! let cluster = CraftyCluster::builder(NodeId(1), Counter::default())
//!     .members([NodeId(1), NodeId(2), NodeId(3)])
//!     .tick_period(Duration::from_millis(10))
//!     .start_local(&net)
//!     .await;
//! # let _ = cluster;
//! # }
//! ```
//!
//! See the `docs/` directory for architecture and design decision records.

mod app;
mod builder;
mod certs;
mod cluster;
mod consumer;
pub mod discovery;
mod env_config;
#[cfg(feature = "http-jobs")]
mod gateway;
mod handler;
mod multi_raft;
mod observer;
mod ready;
mod saga;
mod security;
mod two_phase;
mod workflow;

#[doc(inline)]
pub use crafty_proto::{self as proto, NodeId, PROTOCOL_VERSION, Term};

#[doc(inline)]
pub use {crafty_actor as actor, crafty_client as client, crafty_core as core};

#[doc(inline)]
pub use {crafty_dashboard as dashboard, crafty_macros as macros, crafty_net as net};

#[doc(inline)]
pub use crafty_storage as storage;

pub use app::{CraftyApp, CraftyAppBuilder, EmptyStateMachine, WorkerInfo};
pub use builder::{CraftyClusterBuilder, StartError};
pub use certs::{CertReloadError, CertReloadHandle, PemSecurity, ReloadOpts, cert_paths_from_env};
pub use cluster::{AddRaftGroupsError, ClusterFacts, CraftyCluster, LeaveError, ScaleClusterError};
pub use consumer::{ConsumerOpts, JobConsumer};
pub use crafty_actor::{
    ActorSession, AutoscalePolicy, ClusterActorStateStore, ClusterJobQueue, DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_QUEUE_BATCH_MAX, DEFAULT_QUEUE_PREFETCH, DirectoryPolicy, DirectoryRetry,
    EnqueueOptions, InMemoryJobQueue, InMemoryMailboxSpool, JobId, JobQueue, LeaseId, LeasedJob,
    MailboxSpool, MembershipAutoscalePolicy, QueueError, QueueMetrics, QueueService, RecurringJob,
    RedbActorStateStore, RedbJobQueue, RedbMailboxSpool, ShardedJobQueue, StoreService, WorkerId,
    run_queue_autoscaler, run_queue_consumer, run_queue_membership_autoscaler,
    run_queue_schedule_ticker,
};
pub use crafty_actor::{ResourceProfile, VpsResources};
pub use crafty_core::ReachabilityConfig;
pub use crafty_core::kv;
pub use crafty_core::kv::{Kv, KvCommand, KvError, KvMachine, KvQuery, KvResponse};
pub use crafty_core::{CompactionPolicy, DEFAULT_COMPACT_BYTES, DEFAULT_COMPACT_ENTRIES};
#[cfg(feature = "http-jobs")]
pub use crafty_http::{
    SagaBody, WorkflowAccepted, WorkflowsApi, WorkflowsApiError, spawn_workflows_server,
};
pub use crafty_macros::consumer;
pub use env_config::{
    AppConfig, NodeRole, app_config_from_env, consumers_enabled_from_env, gateway_only_from_env,
    node_role_from_env, workers_enabled_from_env,
};
#[cfg(feature = "http-jobs")]
pub use gateway::{CraftyGatewayState, GatewayConfig, build_gateway_router, spawn_gateway};
pub use ready::ReadyOpts;
pub use saga::{
    CompositeSagaJournal, Group0SagaJournal, MetaRaftSagaJournal, SagaRegistry, StoreSagaJournal,
    record_saga_metrics, saga_metrics_callback,
};
pub use security::Security;
pub use two_phase::{
    CompositeTwoPhaseJournal, MetaRaftTwoPhaseJournal, StoreTwoPhaseJournal, TwoPhaseRegistry,
    record_two_phase_event, record_two_phase_gc_aborted, record_two_phase_metrics,
    two_phase_metrics_callback,
};
pub use workflow::{WorkflowBuildError, WorkflowBuilder};

/// The peer address book ([`NodeId`] → socket) used to dial cluster members
/// over QUIC. Re-exported for building [`CraftyClusterBuilder::start_quic`] args.
#[doc(no_inline)]
pub use crafty_net::PeerDirectory;
#[doc(no_inline)]
pub use crafty_net::{CertPaths, load_pem_material};

/// Commonly used telemetry/observability types, re-exported for convenience.
#[doc(no_inline)]
pub use crafty_dashboard::{
    CraftyEvent, EventBus, EventSubscription, Metrics, StopReason, TraceOpts, init_tracing,
};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
