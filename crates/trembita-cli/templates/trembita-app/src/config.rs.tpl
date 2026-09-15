//! Typed configuration from environment variables.

use std::net::SocketAddr;
use std::path::PathBuf;

use trembita::env::product_http_from_wire;

/// Runtime configuration for {{PROJECT_NAME}}.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// redb + snapshot directory (`TREMBITA_DATA_DIR`).
    pub data_dir: PathBuf,
    /// QUIC wire bind (`TREMBITA_LISTEN`).
    pub wire_addr: SocketAddr,
    /// Product + ops TCP — same host:port as [`Self::wire_addr`] unless `TREMBITA_HTTP=-`.
    pub http_addr: Option<SocketAddr>,
}

impl AppConfig {
    /// Load from environment with local-dev defaults.
    pub fn from_env() -> Self {
        let wire_addr = env_addr("TREMBITA_LISTEN", "127.0.0.1:443");
        let http_addr = product_http_from_wire(wire_addr)
            .expect("TREMBITA_HTTP must match TREMBITA_LISTEN or be `-`");
        Self {
            data_dir: env_path("TREMBITA_DATA_DIR", "/tmp/{{PROJECT_NAME}}"),
            wire_addr,
            http_addr,
        }
    }
}

fn env_path(key: &str, default: &str) -> PathBuf {
    std::env::var(key)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(default))
}

fn env_addr(key: &str, default: &str) -> SocketAddr {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| default.parse().expect("valid default addr"))
}
