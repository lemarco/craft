//! `craft-net` — HTTP/3 over QUIC transport with mTLS (ADR 010, ADR 006).
//!
//! This crate owns the on-the-wire contract and, in later increments, the
//! `quinn`/`h3` server, the rustls mTLS configuration, and the peer connection
//! pool (backlog Track C).
//!
//! Landed so far (transport-agnostic core):
//!
//! * [`route`] — the fixed `/raft/v1/*` route table and per-route
//!   [`TrafficClass`] used for peer-connection isolation (ADR 027 R2).
//! * [`wire`] — `postcard` body framing: content-type, protocol-version, and a
//!   16 MiB body-size guard.
//! * [`peer`] — the [`PeerDirectory`] address book and route-URL builder.

pub use craft_proto as proto;

pub mod peer;
pub mod route;
pub mod wire;

pub use peer::PeerDirectory;
pub use route::{Route, TrafficClass};
pub use wire::{WireError, decode_body, encode_body};
