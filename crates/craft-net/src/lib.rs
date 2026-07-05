//! `craft-net` — HTTP/3 over QUIC transport with mTLS (ADR 010, ADR 006).
//!
//! Provides the `Transport` trait, a `quinn`/`h3` server, route dispatch, and a
//! peer connection pool isolated from client traffic (backlog Track C).

pub use craft_proto as proto;
