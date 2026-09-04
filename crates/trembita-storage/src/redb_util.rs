//! Shared helpers for redb-backed product adapters (queue, topic, actor store, mailbox spool).

use std::fmt::Display;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::Database;

/// Wall-clock milliseconds since UNIX epoch (saturating on overflow).
#[must_use]
pub fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Stringify a backend/codec error for typed store/queue/topic error enums.
#[must_use]
pub fn error_string(e: impl Display) -> String {
    e.to_string()
}

/// Open or create a redb database wrapped in [`Mutex`] (async adapter pattern).
///
/// # Errors
/// Returns the backend error string when `Database::create` fails.
pub fn open_mutex_database(path: impl AsRef<Path>) -> Result<Mutex<Database>, String> {
    Database::create(path).map(Mutex::new).map_err(error_string)
}
