//! # craft
//!
//! A library-first distributed framework: write your state machine and actors
//! once, then run the same binary on as many nodes as you like. Nodes form a
//! Raft cluster over HTTP/3 (mTLS), replicate a linearizable state machine, and
//! host supervised actors that can message, spawn, and migrate across nodes.
//!
//! This facade re-exports the stable public API; most users depend only on
//! `craft` (library-and-publishing). The [`CraftCluster`] builder is the main entry point:
//!
//! ```no_run
//! use std::time::Duration;
//! use craft::{CraftCluster, NodeId};
//! use craft::net::LocalNetwork;
//! # use craft::core::{Config, StateMachine};
//! # use craft::proto::LogIndex;
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
//! let cluster = CraftCluster::builder(NodeId(1), Counter::default())
//!     .members([NodeId(1), NodeId(2), NodeId(3)])
//!     .tick_period(Duration::from_millis(10))
//!     .start_local(&net)
//!     .await;
//! # let _ = cluster;
//! # }
//! ```
//!
//! See the `docs/` directory for architecture and the accepted ADRs.

mod builder;
mod certs;
mod cluster;
pub mod discovery;
mod handler;
mod multi_raft;
mod observer;
mod saga;
mod security;

#[doc(inline)]
pub use craft_proto::{self as proto, NodeId, PROTOCOL_VERSION, Term};

#[doc(inline)]
pub use {craft_actor as actor, craft_client as client, craft_core as core};

#[doc(inline)]
pub use {craft_dashboard as dashboard, craft_macros as macros, craft_net as net};

#[doc(inline)]
pub use craft_storage as storage;

pub use builder::{CraftClusterBuilder, StartError};
pub use certs::{CertReloadError, CertReloadHandle, PemSecurity, ReloadOpts, cert_paths_from_env};
pub use cluster::{ClusterFacts, CraftCluster, LeaveError, ScaleClusterError};
pub use craft_actor::{ActorSession, DEFAULT_DRAIN_TIMEOUT, DirectoryPolicy, DirectoryRetry};
pub use craft_actor::{ResourceProfile, VpsResources};
pub use craft_core::ReachabilityConfig;
pub use saga::{StoreSagaJournal, record_saga_metrics};
pub use security::Security;

/// The peer address book ([`NodeId`] → socket) used to dial cluster members
/// over QUIC. Re-exported for building [`CraftClusterBuilder::start_quic`] args.
#[doc(no_inline)]
pub use craft_net::PeerDirectory;
#[doc(no_inline)]
pub use craft_net::{CertPaths, load_pem_material};

/// Commonly used telemetry/observability types, re-exported for convenience.
#[doc(no_inline)]
pub use craft_dashboard::{
    CraftEvent, EventBus, EventSubscription, Metrics, StopReason, TraceOpts, init_tracing,
};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
