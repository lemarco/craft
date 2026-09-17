//! Cluster readiness polling helpers.

use std::time::Duration;

/// Options for [`TrembitaCluster::wait_until_ready`](crate::TrembitaCluster::wait_until_ready).
#[derive(Debug, Clone)]
pub struct ReadyOpts {
    /// Maximum time to wait before returning `false`.
    pub timeout: Duration,
    /// When non-empty, every listed job stream must be mounted before ready.
    pub job_streams: Vec<String>,
    /// When true, wait until join-pipeline pool readiness (LB `/ready`) instead of Raft leader only.
    pub pool_membership: bool,
}

impl Default for ReadyOpts {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            job_streams: Vec::new(),
            pool_membership: false,
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

    /// Joiners: block boot until `/ready` would return 200 (B-35 pipeline), not until this node is leader.
    #[must_use]
    pub fn pool_membership(mut self) -> Self {
        self.pool_membership = true;
        self
    }
}
