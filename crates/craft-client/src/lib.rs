//! `craft-client` — client handles for talking to a craft cluster (ADR 002).
//!
//! Two layers over a shared [`craft_proto`] wire contract (ADR 002):
//!
//! * **In-process (L1):** embed a node and use its `craft_actor::NodeHandle`
//!   directly (`propose`/`query`) — no serialization, no network.
//! * **Remote (L2):** [`RemoteClient`] speaks `postcard` over any
//!   [`craft_net`] transport (live QUIC/HTTP/3 with client mTLS, or the
//!   in-memory `LocalNetwork` in tests). A follower transparently forwards to
//!   the leader server-side (ADR 003), so a client can connect to any node; the
//!   built-in [`RetryPolicy`] adds failover + leader-follow for elections and
//!   downed nodes.
//!
//! [`TypedClient`] wraps either layer with a
//! [`StateMachine`](craft_core::StateMachine)'s command/query/response types.

pub use {craft_core, craft_net, craft_proto};

mod error;
mod remote;
mod typed;

pub use error::ClientError;
pub use remote::{Client, RemoteClient, RetryPolicy};
pub use typed::TypedClient;
