//! Saga journal backed by [`ActorStateStore`] and Prometheus helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use craft_actor::ActorStateStore;
use craft_client::{
    SagaEvent, SagaJournal, SagaJournalError, SagaJournalPhase, SagaJournalRecord,
    decode_journal_record, encode_journal_record,
};
use craft_dashboard::Metrics;

fn journal_key(saga_id: &[u8]) -> String {
    format!("craft:saga:{}", String::from_utf8_lossy(saga_id))
}

/// Persist saga progress in an external workflow store (Redis / in-memory).
pub struct StoreSagaJournal {
    store: Arc<dyn ActorStateStore>,
}

impl StoreSagaJournal {
    /// Wrap `store` for saga journaling.
    #[must_use]
    pub fn new(store: Arc<dyn ActorStateStore>) -> Self {
        Self { store }
    }

    async fn write(
        &self,
        saga_id: &[u8],
        phase: SagaJournalPhase,
        completed: u32,
        catalog_version: Option<u32>,
    ) -> Result<(), SagaJournalError> {
        let record = SagaJournalRecord {
            saga_id: saga_id.to_vec(),
            phase,
            completed_steps: completed,
            catalog_version,
        };
        let bytes = encode_journal_record(&record)?;
        self.store
            .set(&journal_key(saga_id), &bytes, None)
            .await
            .map_err(|e| SagaJournalError::Backend(e.to_string()))
    }

    async fn read(&self, saga_id: &[u8]) -> Result<Option<SagaJournalRecord>, SagaJournalError> {
        let Some(bytes) = self
            .store
            .get(&journal_key(saga_id))
            .await
            .map_err(|e| SagaJournalError::Backend(e.to_string()))?
        else {
            return Ok(None);
        };
        decode_journal_record(&bytes).map(Some)
    }
}

impl SagaJournal for StoreSagaJournal {
    fn on_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        _steps: usize,
        catalog_version: Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.write(saga_id, SagaJournalPhase::Running, 0, catalog_version)
                .await
        })
    }

    fn on_step_committed<'a>(
        &'a self,
        saga_id: &'a [u8],
        step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            let catalog_version = self.read(saga_id).await?.and_then(|r| r.catalog_version);
            self.write(
                saga_id,
                SagaJournalPhase::Running,
                step as u32 + 1,
                catalog_version,
            )
            .await
        })
    }

    fn on_completed<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            let prev = self.read(saga_id).await?;
            let (completed, catalog_version) = prev
                .map(|r| (r.completed_steps, r.catalog_version))
                .unwrap_or((0, None));
            self.write(
                saga_id,
                SagaJournalPhase::Completed,
                completed,
                catalog_version,
            )
            .await
        })
    }

    fn on_compensation_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        _failed_step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            let prev = self.read(saga_id).await?;
            let (completed, catalog_version) = prev
                .map(|r| (r.completed_steps, r.catalog_version))
                .unwrap_or((0, None));
            self.write(
                saga_id,
                SagaJournalPhase::Compensating,
                completed,
                catalog_version,
            )
            .await
        })
    }

    fn on_compensated<'a>(
        &'a self,
        saga_id: &'a [u8],
        _compensated_steps: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            let prev = self.read(saga_id).await?;
            let (completed, catalog_version) = prev
                .map(|r| (r.completed_steps, r.catalog_version))
                .unwrap_or((0, None));
            self.write(
                saga_id,
                SagaJournalPhase::Compensated,
                completed,
                catalog_version,
            )
            .await
        })
    }

    fn on_stuck<'a>(
        &'a self,
        saga_id: &'a [u8],
        _failed_step: usize,
        _compensate_failed_at: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            let prev = self.read(saga_id).await?;
            let (completed, catalog_version) = prev
                .map(|r| (r.completed_steps, r.catalog_version))
                .unwrap_or((0, None));
            self.write(saga_id, SagaJournalPhase::Stuck, completed, catalog_version)
                .await
        })
    }
}

/// Increment Prometheus counters for [`SagaEvent`] (ADR Phase 4).
pub fn record_saga_metrics(metrics: &Metrics, node_id: u64, event: SagaEvent) {
    let node = node_id.to_string();
    let labels = [("node", node.as_str())];
    match event {
        SagaEvent::Completed { .. } => {
            metrics.incr(
                "craft_saga_completed_total",
                "Cross-shard sagas completed (all forward steps committed)",
                &labels,
                1.0,
            );
        }
        SagaEvent::Compensated { .. } => {
            metrics.incr(
                "craft_saga_compensated_total",
                "Cross-shard sagas compensated after forward failure",
                &labels,
                1.0,
            );
        }
        SagaEvent::Stuck { .. } => {
            metrics.incr(
                "craft_saga_stuck_total",
                "Cross-shard sagas stuck (compensation failed)",
                &labels,
                1.0,
            );
        }
    }
}
