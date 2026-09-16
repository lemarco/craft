use std::net::SocketAddr;

use trembita_net::TransportError;

/// An error starting a node over the live QUIC transport
/// ([`start_quic`](super::TrembitaClusterBuilder::start_quic)).
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The mTLS server/client configuration could not be built.
    #[error("tls configuration: {0}")]
    Tls(#[from] trembita_net::TlsError),

    /// The QUIC listener could not bind `addr`.
    #[error("bind {addr}: {source}")]
    Bind {
        /// The address the listener tried to bind.
        addr: SocketAddr,
        /// The underlying transport error.
        source: TransportError,
    },

    /// A dynamic join via [`join`](super::TrembitaClusterBuilder::join) could not be
    /// completed (seed unreachable, no leader, or the cluster refused it).
    #[error("cluster join failed: {0}")]
    Join(String),

    /// Environment or app configuration could not be parsed.
    #[error("configuration: {0}")]
    Config(String),
}
