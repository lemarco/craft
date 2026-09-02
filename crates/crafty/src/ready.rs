//! Cluster readiness polling helpers.

use std::time::Duration;

/// Options for [`CraftyCluster::wait_until_ready`](crate::cluster::CraftyCluster::wait_until_ready).
#[derive(Debug, Clone)]
pub struct ReadyOpts {
    /// Maximum time to wait before returning `false`.
    pub timeout: Duration,
    /// When non-empty, every listed job stream must be mounted before ready.
    pub job_streams: Vec<String>,
}

impl Default for ReadyOpts {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            job_streams: Vec::new(),
        }
    }
}

impl ReadyOpts {
    /// Wait until this job stream is registered (job queue gateways).
    #[must_use]
    pub fn with_queue(mut self, stream: impl Into<String>) -> Self {
        self.job_streams.push(stream.into());
        self
    }
}
