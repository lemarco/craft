//! Shared timing and networking helpers for fast integration tests.

use std::net::SocketAddr;
use std::time::Duration;

use trembita_core::Config;
use trembita_proto::LogicalTick;

/// Default runtime tick used when spawning test clusters.
pub const TICK_PERIOD: Duration = Duration::from_millis(10);

/// Fast election/heartbeat timings for integration tests (seed `7`).
#[must_use]
pub fn fast_raft_config() -> Config {
    fast_raft_config_with_seed(7)
}

/// Fast election/heartbeat timings with an explicit RNG seed.
#[must_use]
pub fn fast_raft_config_with_seed(seed: u64) -> Config {
    Config {
        election_timeout_min: LogicalTick(5),
        election_timeout_max: LogicalTick(10),
        heartbeat_interval: LogicalTick(2),
        seed,
        ..Default::default()
    }
}

/// Bind an ephemeral localhost UDP port (for QUIC listeners in tests).
///
/// # Panics
/// If binding to `127.0.0.1:0` fails.
#[must_use]
pub fn free_udp() -> SocketAddr {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind udp");
    sock.local_addr().expect("local addr")
}

/// Optional test setup: install the tracing subscriber when log env vars are set.
pub fn test_setup() {
    if std::env::var_os("TREMBITA_LOG_REBALANCE").is_some()
        || std::env::var_os("RUST_LOG").is_some()
        || std::env::var_os("TREMBITA_LOG").is_some()
    {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .try_init();
    }
}
