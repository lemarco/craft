//! Per-node workload runtime wired by `TrembitaClusterBuilder::workload`.

use std::sync::Arc;

use tokio::sync::watch;
use trembita_jobs::ConsumerTune;
use trembita_runtime::ComputeTokenPool;

use crate::connections::{ConnectionTracker, HttpInFlight};
use trembita_jobs::ConsumerInflight;

/// Shared workload governor state on a running cluster node.
#[derive(Debug)]
pub struct WorkloadRuntime {
    pool: Arc<ComputeTokenPool>,
    tune: watch::Receiver<ConsumerTune>,
    connections: Arc<ConnectionTracker>,
    http_inflight: Arc<HttpInFlight>,
    consumer_inflight: Arc<ConsumerInflight>,
    _stop_tx: watch::Sender<bool>,
}

impl WorkloadRuntime {
    pub(crate) fn new(
        pool: Arc<ComputeTokenPool>,
        tune: watch::Receiver<ConsumerTune>,
        connections: Arc<ConnectionTracker>,
        http_inflight: Arc<HttpInFlight>,
        consumer_inflight: Arc<ConsumerInflight>,
        stop_tx: watch::Sender<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            tune,
            connections,
            http_inflight,
            consumer_inflight,
            _stop_tx: stop_tx,
        })
    }

    /// Process-wide compute token pool.
    #[must_use]
    pub fn pool(&self) -> Arc<ComputeTokenPool> {
        Arc::clone(&self.pool)
    }

    /// Subscribe to live consumer tuning.
    #[must_use]
    pub fn tune(&self) -> watch::Receiver<ConsumerTune> {
        self.tune.clone()
    }

    /// Gateway connection tracker used by the governor.
    #[must_use]
    pub fn connections(&self) -> Arc<ConnectionTracker> {
        Arc::clone(&self.connections)
    }

    /// In-flight HTTP handler counter used by the governor.
    #[must_use]
    pub fn http_inflight(&self) -> Arc<HttpInFlight> {
        Arc::clone(&self.http_inflight)
    }

    /// Aggregated in-flight consumer handlers on this node.
    #[must_use]
    pub fn consumer_inflight(&self) -> Arc<ConsumerInflight> {
        Arc::clone(&self.consumer_inflight)
    }

    #[doc(hidden)]
    pub fn queue_consumer_workload(&self) -> trembita_jobs::QueueConsumerWorkload {
        trembita_jobs::QueueConsumerWorkload {
            tokens: self.pool(),
            tune: self.tune(),
            consumer_inflight: Some(self.consumer_inflight()),
        }
    }
}
