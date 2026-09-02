//! Showcase debug logging (`target: "showcase"`).
//!
//! Enable: `RUST_LOG=showcase=debug` or run via `./cluster.sh` (sets default filter).

pub const NAME: &str = "background-jobs";

pub fn init_tracing() {
    trembita::init_tracing();
    tracing::debug!(target: "showcase", showcase = NAME, "tracing initialized");
}

pub fn startup(mode: &str, node_id: u64, data_dir: &std::path::Path) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        mode,
        node_id,
        data_dir = %data_dir.display(),
        gateway = ?std::env::var("TREMBITA_GATEWAY").ok(),
        peers = ?std::env::var("TREMBITA_PEERS").ok(),
        consumer = true,
        "showcase starting"
    );
}

#[allow(dead_code)]
pub fn cluster_ready() {
    tracing::debug!(target: "showcase", showcase = NAME, "cluster ready (leader + queue)");
}

pub fn worker_job(instance: u32, payload_len: usize, payload_preview: &str) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        node_id = ?std::env::var("TREMBITA_NODE_ID").ok(),
        instance,
        payload_len,
        payload_preview,
        "consumer handling job"
    );
}

pub fn shutdown(consumers: usize) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        consumers,
        "showcase shutting down"
    );
}
