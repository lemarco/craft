//! Shared helpers for product showcases (`examples/*`).

use std::path::PathBuf;

/// True when `CRAFTY_PEERS` is set (QUIC cluster mode).
#[must_use]
pub fn cluster_mode() -> bool {
    std::env::var("CRAFTY_PEERS")
        .ok()
        .is_some_and(|v| !v.trim().is_empty())
}

/// Map `0.0.0.0:port` to `127.0.0.1:port` for local browser/curl hints.
#[must_use]
pub fn display_addr(addr: &str) -> String {
    if let Some(port) = addr.strip_prefix("0.0.0.0:") {
        format!("127.0.0.1:{port}")
    } else {
        addr.to_string()
    }
}

/// `CRAFTY_DATA_DIR` or `/tmp/{default_name}`.
#[must_use]
pub fn data_dir(default_name: &str) -> PathBuf {
    std::env::var("CRAFTY_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join(default_name))
}

/// Parse common truthy env flags (`1`, `true`, `yes`, `on`).
#[must_use]
pub fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}
