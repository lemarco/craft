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

    async fn update(
        &self,
        saga_id: &[u8],
        f: impl FnOnce(SagaJournalRecord) -> SagaJournalRecord,
    ) -> Result<(), SagaJournalError> {
        let prev = self.read(saga_id).await?;
        let rec = prev.unwrap_or_else(|| SagaJournalRecord {
            saga_id: saga_id.to_vec(),
            phase: SagaJournalPhase::Running,
            completed_steps: 0,
            catalog_version: None,
            failed_step: None,
            compensate_failed_at: None,
        });
        let updated = f(rec);
        let bytes = encode_journal_record(&updated)?;
        self.store
            .set(&journal_key(saga_id), &bytes, None)
            .await
            .map_err(|e| SagaJournalError::Backend(e.to_string()))
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
            self.update(saga_id, |mut rec| {
                rec.phase = SagaJournalPhase::Running;
                rec.completed_steps = 0;
                rec.catalog_version = catalog_version;
                rec.failed_step = None;
                rec.compensate_failed_at = None;
                rec
            })
            .await
        })
    }

    fn on_step_committed<'a>(
        &'a self,
        saga_id: &'a [u8],
        step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(saga_id, |mut rec| {
                rec.completed_steps = step as u32 + 1;
                rec
            })
            .await
        })
    }

    fn on_completed<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(saga_id, |mut rec| {
                rec.phase = SagaJournalPhase::Completed;
                rec
            })
            .await
        })
    }

    fn on_compensation_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(saga_id, |mut rec| {
                rec.phase = SagaJournalPhase::Compensating;
                rec.failed_step = Some(failed_step as u32);
                rec
            })
            .await
        })
    }

    fn on_compensated<'a>(
        &'a self,
        saga_id: &'a [u8],
        _compensated_steps: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(saga_id, |mut rec| {
                rec.phase = SagaJournalPhase::Compensated;
                rec.compensate_failed_at = None;
                rec
            })
            .await
        })
    }

    fn on_stuck<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
        compensate_failed_at: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(saga_id, |mut rec| {
                rec.phase = SagaJournalPhase::Stuck;
                rec.failed_step = Some(failed_step as u32);
                rec.compensate_failed_at = Some(compensate_failed_at as u32);
                rec
            })
            .await
        })
    }

    fn load<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<SagaJournalRecord>, SagaJournalError>> + Send + 'a>,
    > {
        Box::pin(async move { self.read(saga_id).await })
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

/// Record one saga lifecycle event on the cluster metrics registry.
pub fn record_saga_event(metrics: &Metrics, node_id: u64, event: SagaEvent) {
    record_saga_metrics(metrics, node_id, event);
}

/// Metrics hook suitable for [`craft_client::RunSagaOpts::on_event`].
#[must_use]
pub fn saga_metrics_callback(
    metrics: Metrics,
    node_id: u64,
) -> Arc<dyn Fn(SagaEvent) + Send + Sync> {
    Arc::new(move |event| record_saga_event(&metrics, node_id, event))
}
