//! # craft
//!
//! A library-first distributed framework: write your state machine and actors
//! once, then run the same binary on as many nodes as you like. Nodes form a
//! Raft cluster over HTTP/3 (mTLS), replicate a linearizable state machine, and
//! host supervised actors that can message, spawn, and migrate across nodes.
//!
//! This facade re-exports the stable public API; most users depend only on
//! `craft` (ADR 028). The `CraftCluster` builder (backlog Wave 4) will be the
//! main entry point.
//!
//! See the `docs/` directory for architecture and the accepted ADRs.

#[doc(inline)]
pub use craft_proto::{self as proto, NodeId, PROTOCOL_VERSION, Term};

#[doc(inline)]
pub use {craft_actor as actor, craft_client as client, craft_core as core};

#[doc(inline)]
pub use {craft_macros as macros, craft_net as net, craft_storage as storage};

/// Library version string (from `Cargo.toml`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
