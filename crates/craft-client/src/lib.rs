//! `craft-client` — client handles for talking to a craft cluster (ADR 002).
//!
//! Provides in-process `ClientHandle`, remote `RemoteClient` over HTTP/3 with
//! client mTLS (ADR 006), and a typed wrapper (backlog Track F).

pub use {craft_net, craft_proto};
