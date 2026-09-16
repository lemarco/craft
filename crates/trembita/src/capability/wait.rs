//! Stored replies for [`super::Route::QueuedWait`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use trembita_jobs::JobId;

fn job_key(job_id: JobId) -> String {
    job_id.0.to_string()
}

/// In-memory wait results keyed by job id (product apps with `data_dir` use the same store).
#[derive(Default)]
pub struct CapWaitStore {
    results: Mutex<HashMap<String, Vec<u8>>>,
}

impl CapWaitStore {
    /// Store a reply for a completed queued job.
    pub fn store(&self, job_id: JobId, bytes: Vec<u8>) {
        self.results
            .lock()
            .expect("cap wait store lock")
            .insert(job_key(job_id), bytes);
    }

    /// Take a stored reply (one-shot).
    pub fn take(&self, job_id: JobId) -> Option<Vec<u8>> {
        self.results
            .lock()
            .expect("cap wait store lock")
            .remove(&job_key(job_id))
    }

    /// Poll until a reply appears or `timeout` elapses.
    pub async fn wait_for(&self, job_id: JobId, timeout: Duration) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(bytes) = self.take(job_id) {
                return Some(bytes);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
