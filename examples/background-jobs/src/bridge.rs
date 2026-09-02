//! Queue → actor bridge: consumers delegate side effects to stateful worker groups.

use std::sync::{Arc, OnceLock};

use crafty::{CraftyApp, proto};

static APP: OnceLock<Arc<CraftyApp>> = OnceLock::new();

/// Called from [`ConsumerOpts::on_app`] before the lease loop starts.
pub fn register(app: Arc<CraftyApp>) {
    let _ = APP.set(app);
}

/// Fire-and-forget notify to the ledger actor group (see `LedgerWorker`).
pub async fn notify_ledger(key: &str) -> Result<(), String> {
    let app = APP.get().ok_or("bridge app not registered")?;
    let payload = proto::encode(&key.to_string()).map_err(|e| e.to_string())?;
    app.cast("ledger", payload).await.map_err(|e| e.to_string())
}
