//! Shared helpers for product showcases (`examples/*`).

use std::net::SocketAddr;
use std::path::PathBuf;

use trembita::env::product_http_from_wire;

/// Map `0.0.0.0:port` to `127.0.0.1:port` for local browser/curl hints.
#[must_use]
pub fn display_addr(addr: &str) -> String {
    if let Some(port) = addr.strip_prefix("0.0.0.0:") {
        format!("127.0.0.1:{port}")
    } else {
        addr.to_string()
    }
}

/// `TREMBITA_DATA_DIR` or `/tmp/{default_name}`.
#[must_use]
pub fn data_dir(default_name: &str) -> PathBuf {
    std::env::var("TREMBITA_DATA_DIR")
        .map_or_else(|_| std::env::temp_dir().join(default_name), PathBuf::from)
}

/// QUIC wire bind (`TREMBITA_LISTEN`, else `default`).
#[must_use]
pub fn wire_bind_from_env(default: &str) -> SocketAddr {
    std::env::var("TREMBITA_LISTEN")
        .unwrap_or_else(|_| default.into())
        .parse()
        .unwrap_or_else(|e| panic!("invalid TREMBITA_LISTEN: {e}"))
}

/// Product + ops TCP bind — **same** socket address as [`wire_bind_from_env`].
///
/// # Panics
/// When env disables HTTP (`TREMBITA_HTTP=-`) or when HTTP env disagrees with `TREMBITA_LISTEN`.
#[must_use]
pub fn http_bind_from_env(default_wire: &str) -> SocketAddr {
    let wire = wire_bind_from_env(default_wire);
    product_http_from_wire(wire)
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|| panic!("TREMBITA_HTTP=- but this showcase requires HTTP"))
}

/// Display string for the unified TCP bind (same port as QUIC wire).
#[must_use]
pub fn http_bind_display(default_wire: &str) -> String {
    http_bind_from_env(default_wire).to_string()
}

/// True when HTTP is explicitly disabled (`TREMBITA_HTTP=-` or `TREMBITA_GATEWAY=-`).
#[must_use]
pub fn http_disabled() -> bool {
    matches!(
        std::env::var("TREMBITA_HTTP")
            .or_else(|_| std::env::var("TREMBITA_GATEWAY"))
            .ok()
            .as_deref(),
        Some("-")
    )
}

/// Parse common truthy env flags (`1`, `true`, `yes`, `on`).
#[must_use]
pub fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}
