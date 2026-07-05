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
//! * [`transport`] — the [`Transport`]/[`RequestHandler`] ports plus the
//!   in-memory [`LocalNetwork`] used by tests and the simulator.
//! * [`tls`] — mTLS `quinn` server/client configs and dev cert generation.
//! * [`quic`] — the live HTTP/3 [`QuicServer`] and [`QuicTransport`].

pub use craft_proto as proto;

pub mod peer;
pub mod quic;
pub mod route;
pub mod tls;
pub mod transport;
pub mod wire;

pub use peer::PeerDirectory;
pub use quic::{QuicServer, QuicTransport, client_endpoint};
pub use route::{Route, TrafficClass};
pub use tls::{NodeIdentity, TlsError, client_config, server_config};
pub use transport::{
    LocalNetwork, RequestHandler, Transport, TransportError, send_client_request,
    send_join_request, send_peer_rpc,
};
pub use wire::{WireError, decode_body, encode_body};

#[cfg(feature = "dev-certs")]
pub use tls::ClusterCa;
