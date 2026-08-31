//! Showcase debug logging (`target: "showcase"`).

pub const NAME: &str = "stateful-workers";

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
        gateway = ?std::env::var("CRAFTY_GATEWAY").ok(),
        peers = ?std::env::var("CRAFTY_PEERS").ok(),
        "showcase starting"
    );
}

#[allow(dead_code)]
pub fn cluster_ready() {
    tracing::debug!(target: "showcase", showcase = NAME, "cluster ready");
}

pub fn order_cast(order_id: u64, gateway: &str) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        order_id,
        gateway,
        "cast client sending order"
    );
}

pub fn order_handle(order_id: u64, idempotent_skip: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        node_id = ?std::env::var("CRAFTY_NODE_ID").ok(),
        order_id,
        idempotent_skip,
        "OrderProcessor handle"
    );
}

pub fn shutdown() {
    tracing::debug!(target: "showcase", showcase = NAME, "showcase shutting down");
}
