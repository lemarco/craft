//! Per-node workload runtime wired by [`crate::cluster::TrembitaClusterBuilder::workload`].

use std::sync::Arc;

use tokio::sync::watch;
use trembita_actor::{ComputeTokenPool, ConsumerTune};

use crate::gateway::ConnectionTracker;

/// Shared workload governor state on a running cluster node.
#[derive(Debug)]
pub struct WorkloadRuntime {
    pool: Arc<ComputeTokenPool>,
    tune: watch::Receiver<ConsumerTune>,
    connections: Arc<ConnectionTracker>,
    _stop_tx: watch::Sender<bool>,
}

impl WorkloadRuntime {
    pub(crate) fn new(
        pool: Arc<ComputeTokenPool>,
        tune: watch::Receiver<ConsumerTune>,
        connections: Arc<ConnectionTracker>,
        stop_tx: watch::Sender<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            tune,
            connections,
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

    pub(crate) fn queue_consumer_workload(&self) -> trembita_actor::QueueConsumerWorkload {
        trembita_actor::QueueConsumerWorkload {
            tokens: self.pool(),
            tune: self.tune(),
        }
    }
}
