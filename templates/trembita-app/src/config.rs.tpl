//! Typed configuration from environment variables.

use std::net::SocketAddr;
use std::path::PathBuf;

/// Runtime configuration for {{PROJECT_NAME}}.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// redb + snapshot directory (`TREMBITA_DATA_DIR`).
    pub data_dir: PathBuf,
    /// Product + ops HTTP bind address (`TREMBITA_HTTP`, fallback `TREMBITA_GATEWAY`).
    pub http_addr: SocketAddr,
}

impl AppConfig {
    /// Load from environment with local-dev defaults.
    pub fn from_env() -> Self {
        Self {
            data_dir: env_path("TREMBITA_DATA_DIR", "/tmp/{{PROJECT_NAME}}"),
            http_addr: env_addr("TREMBITA_HTTP", "127.0.0.1:443"),
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
        .or_else(|_| std::env::var("TREMBITA_GATEWAY"))
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| default.parse().expect("valid default addr"))
}
