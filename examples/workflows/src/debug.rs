//! Showcase debug logging (`target: "showcase"`).

pub const NAME: &str = "workflows";

pub fn init_tracing() {
    crafty::init_tracing();
    tracing::debug!(target: "showcase", showcase = NAME, "tracing initialized");
}

pub fn startup(mode: &str, node_id: u64, data_dir: &std::path::Path) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        mode,
        node_id,
        data_dir = %data_dir.display(),
        trigger = ?std::env::var("CRAFTY_TRIGGER").ok(),
        peers = ?std::env::var("CRAFTY_PEERS").ok(),
        "showcase starting"
    );
}

pub fn cluster_ready() {
    tracing::debug!(target: "showcase", showcase = NAME, "cluster ready");
}

pub fn saga_run(saga_id: &str, local: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        saga_id,
        local,
        "running keyed saga"
    );
}

pub fn saga_resume(saga_id: &str, local: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        saga_id,
        local,
        "resuming keyed saga"
    );
}

pub fn saga_outcome(saga_id: &str, outcome: &str) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        saga_id,
        outcome,
        "saga finished"
    );
}

pub fn http_trigger(path: &str, saga_id: &str) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        path,
        saga_id,
        "HTTP trigger received"
    );
}

pub fn shutdown() {
    tracing::debug!(target: "showcase", showcase = NAME, "showcase shutting down");
}
