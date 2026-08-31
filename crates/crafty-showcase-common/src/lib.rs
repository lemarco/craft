//! Shared helpers for product showcases (`examples/*`).

use std::path::PathBuf;

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
        .map_or_else(|_| std::env::temp_dir().join(default_name), PathBuf::from)
}

/// Parse common truthy env flags (`1`, `true`, `yes`, `on`).
#[must_use]
pub fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}
