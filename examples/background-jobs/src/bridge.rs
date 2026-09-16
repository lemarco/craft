//! Queue → capability bridge: consumers delegate side effects to `ledger.record`.

use std::sync::{Arc, OnceLock};

use trembita::{CapVia, TrembitaApp};

use crate::capabilities::ledger::Record;

static APP: OnceLock<Arc<TrembitaApp>> = OnceLock::new();

/// Called from [`ConsumerOpts::on_app`] before the lease loop starts.
pub fn register(app: Arc<TrembitaApp>) {
    let _ = APP.set(app);
}

/// Fire-and-forget notify to the ledger capability group.
pub async fn notify_ledger(key: &str) -> Result<(), String> {
    let app = APP.get().ok_or("bridge app not registered")?;
    Record {
        key: key.to_string(),
    }
    .via(app.as_ref())
    .fire()
    .await
    .map_err(|e| e.to_string())
}
