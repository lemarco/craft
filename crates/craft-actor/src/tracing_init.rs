//! Global `tracing` subscriber setup (observability §1).
//!
//! Binaries and tests call [`init_tracing`] once at startup. Filter directives
//! come from `RUST_LOG`, then `CRAFT_LOG` if `RUST_LOG` is unset. The legacy
//! `CRAFT_LOG_REBALANCE=1` knob adds `craft::rebalance=debug` to the filter.

use std::sync::OnceLock;

static INIT: OnceLock<()> = OnceLock::new();

/// Install the process-wide `tracing` subscriber (stderr, `EnvFilter`).
///
/// Idempotent: later calls are no-ops. Safe from tests and binaries.
pub fn init_tracing() {
    INIT.get_or_init(|| {
        let filter = env_filter();
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(false)
            .try_init();
    });
}

fn env_filter() -> tracing_subscriber::EnvFilter {
    use tracing_subscriber::EnvFilter;

    let mut directives = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("CRAFT_LOG"))
        .unwrap_or_else(|_| "warn".into());

    if std::env::var_os("CRAFT_LOG_REBALANCE").is_some() {
        if !directives.is_empty() {
            directives.push(',');
        }
        directives.push_str("craft::rebalance=debug");
    }

    EnvFilter::try_new(&directives).unwrap_or_else(|_| EnvFilter::new("warn"))
}
