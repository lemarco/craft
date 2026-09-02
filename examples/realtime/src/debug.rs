//! Showcase debug logging (`target: "showcase"`).

pub const NAME: &str = "realtime";

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
        "showcase starting"
    );
}

#[allow(dead_code)]
pub fn cluster_ready() {
    tracing::debug!(target: "showcase", showcase = NAME, "cluster ready");
}

pub fn ws_connect(user: &str, token_ok: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        user,
        token_ok,
        "WebSocket upgrade"
    );
}

pub fn session_open(user: &str, ok: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        user,
        ok,
        "ActorSession open"
    );
}

pub fn ws_message(user: &str, text: &str, cast_ok: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        user,
        text,
        cast_ok,
        "WebSocket message cast"
    );
}

pub fn http_message(user: &str, text: &str, cast_ok: bool) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        user,
        text,
        cast_ok,
        "HTTP chat cast"
    );
}

#[allow(dead_code)] // wired from WS reconnect path when added
pub fn session_reconnect(user: &str, attempt: u32) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        user,
        attempt,
        "session reconnect after recoverable error"
    );
}

pub fn chat_message(text: &str) {
    tracing::debug!(
        target: "showcase",
        showcase = NAME,
        node_id = ?std::env::var("TREMBITA_NODE_ID").ok(),
        text,
        "ChatWorker received message"
    );
}

pub fn shutdown() {
    tracing::debug!(target: "showcase", showcase = NAME, "showcase shutting down");
}
