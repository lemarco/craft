//! `crafty-net` — HTTP/3 over QUIC transport with mTLS (wire-transport, security).
//!
//! This crate owns the on-the-wire contract and, in later increments, the
//! `quinn`/`h3` server, the rustls mTLS configuration, and the peer connection
//! pool (backlog Track C).
//!
//! Landed so far (transport-agnostic core):
//!
//! * [`route`] — the fixed `/raft/v1/*` route table and per-route
//!   [`TrafficClass`] used for peer-connection isolation (future-work-and-risks R2).
//! * [`wire`] — `postcard` body framing: content-type, protocol-version, and a
//!   16 MiB body-size guard.
//! * [`peer`] — the [`PeerDirectory`] address book and route-URL builder.
//! * [`transport`] — the [`Transport`]/[`RequestHandler`] ports plus the
//!   in-memory [`LocalNetwork`] used by tests and the simulator.
//! * [`tls`] — mTLS `quinn` server/client configs and dev cert generation.
//! * [`quic`] — the live HTTP/3 [`QuicServer`] and [`QuicTransport`].
//! * [`pem`] — load operator PEM material and detect on-disk rotation (cert-automation).

pub use crafty_proto as proto;

pub mod backoff;
pub mod group_transport;
pub mod peer;
pub mod pem;
pub mod priority;
pub mod quic;
pub mod route;
pub mod tls;
pub mod transport;
pub mod wire;

pub use backoff::BackoffPolicy;
pub use group_transport::GroupTransport;
pub use peer::PeerDirectory;
pub use pem::{CertFingerprint, CertPaths, PemMaterial, load_pem_material};
pub use priority::TrafficPolicy;
pub use quic::{QuicServer, QuicTransport, client_endpoint};
pub use route::{Route, TrafficClass};
pub use tls::{NodeIdentity, TlsError, client_config, node_id_from_server_name, server_config};
pub use transport::{
    LocalNetwork, LocalTransport, RemoteError, RequestHandler, Transport, TransportError,
    fetch_peers, send_actor_deliver, send_actor_migrate, send_actor_scale, send_actor_spawn,
    send_actor_stop, send_catalog_add_request, send_client_request, send_directory_update,
    send_group_migrate, send_join_request, send_leave_request, send_peer_rpc, send_queue_ack,
    send_queue_enqueue, send_queue_lease, send_queue_metrics, send_queue_nack,
    send_queue_replicate, send_store_compare_and_set, send_store_delete, send_store_replicate,
    send_store_set,
};
pub use wire::{WireError, decode_body, encode_body};

#[cfg(feature = "dev-certs")]
pub use tls::ClusterCa;
