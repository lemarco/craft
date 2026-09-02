//! Saga journal backed by Meta-Raft metadata and/or [`ActorStateStore`].

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use trembita_actor::{ActorStateStore, NodeHandle};
use trembita_client::{
    SagaEvent, SagaJournal, SagaJournalError, SagaJournalPhase, SagaJournalRecord,
    decode_journal_record, encode_journal_record,
};
use trembita_core::StateMachine;
use trembita_dashboard::Metrics;
use trembita_proto::SagaJournalCommand;

fn fresh_record(saga_id: &[u8]) -> SagaJournalRecord {
    SagaJournalRecord {
        saga_id: saga_id.to_vec(),
        phase: SagaJournalPhase::Running,
        completed_steps: 0,
        catalog_version: None,
        failed_step: None,
        compensate_failed_at: None,
    }
}

fn journal_key(saga_id: &[u8]) -> String {
    format!("trembita:saga:{}", String::from_utf8_lossy(saga_id))
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
        let rec = prev.unwrap_or_else(|| fresh_record(saga_id));
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
                rec.completed_steps = u32::try_from(step).expect("step index fits u32") + 1;
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
                rec.failed_step = Some(u32::try_from(failed_step).expect("step index fits u32"));
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
                rec.failed_step = Some(u32::try_from(failed_step).expect("step index fits u32"));
                rec.compensate_failed_at =
                    Some(u32::try_from(compensate_failed_at).expect("step index fits u32"));
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

/// In-memory view of saga records applied from Meta-Raft (all replicas).
pub type SagaRegistry = Arc<Mutex<BTreeMap<Vec<u8>, SagaJournalRecord>>>;

type SagaJournalUpsertFn = dyn Fn(SagaJournalCommand) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send>>
    + Send
    + Sync;

/// Persist saga progress in Meta-Raft coordinator metadata (no Redis required).
pub struct MetaRaftSagaJournal {
    upsert: Arc<SagaJournalUpsertFn>,
    registry: SagaRegistry,
}

impl MetaRaftSagaJournal {
    /// Build a journal that proposes upserts on the Meta-Raft group and reads `registry`.
    #[must_use]
    pub fn new<M: StateMachine + 'static>(meta: NodeHandle<M>, registry: SagaRegistry) -> Self {
        let upsert = Arc::new(move |command: SagaJournalCommand| {
            let meta = meta.clone();
            Box::pin(async move {
                meta.upsert_saga_journal(command)
                    .await
                    .map_err(|e| SagaJournalError::Backend(e.to_string()))
            }) as Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send>>
        });
        Self { upsert, registry }
    }

    fn read(&self, saga_id: &[u8]) -> Option<SagaJournalRecord> {
        self.registry.lock().expect("lock").get(saga_id).cloned()
    }

    async fn update(
        &self,
        saga_id: &[u8],
        f: impl FnOnce(SagaJournalRecord) -> SagaJournalRecord,
    ) -> Result<(), SagaJournalError> {
        let prev = self.read(saga_id);
        let updated = f(prev.unwrap_or_else(|| fresh_record(saga_id)));
        let command = SagaJournalCommand {
            saga_id: saga_id.to_vec(),
            record: encode_journal_record(&updated)?,
        };
        (self.upsert)(command).await
    }
}

impl SagaJournal for MetaRaftSagaJournal {
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
                rec.completed_steps = u32::try_from(step).expect("step index fits u32") + 1;
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
                rec.failed_step = Some(u32::try_from(failed_step).expect("step index fits u32"));
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
                rec.failed_step = Some(u32::try_from(failed_step).expect("step index fits u32"));
                rec.compensate_failed_at =
                    Some(u32::try_from(compensate_failed_at).expect("step index fits u32"));
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
        Box::pin(async move { Ok(self.read(saga_id)) })
    }
}

/// Replicate to Meta-Raft and optionally mirror in an external store (Redis).
pub struct CompositeSagaJournal {
    meta: MetaRaftSagaJournal,
    store: Option<StoreSagaJournal>,
}

impl CompositeSagaJournal {
    /// Meta-Raft is always the durable fallback; `store` is an optional mirror.
    #[must_use]
    pub fn new(meta: MetaRaftSagaJournal, store: Option<StoreSagaJournal>) -> Self {
        Self { meta, store }
    }
}

impl SagaJournal for CompositeSagaJournal {
    fn on_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        steps: usize,
        catalog_version: Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta
                .on_started(saga_id, steps, catalog_version)
                .await?;
            if let Some(store) = &self.store {
                store.on_started(saga_id, steps, catalog_version).await?;
            }
            Ok(())
        })
    }

    fn on_step_committed<'a>(
        &'a self,
        saga_id: &'a [u8],
        step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta.on_step_committed(saga_id, step).await?;
            if let Some(store) = &self.store {
                store.on_step_committed(saga_id, step).await?;
            }
            Ok(())
        })
    }

    fn on_completed<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta.on_completed(saga_id).await?;
            if let Some(store) = &self.store {
                store.on_completed(saga_id).await?;
            }
            Ok(())
        })
    }

    fn on_compensation_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta
                .on_compensation_started(saga_id, failed_step)
                .await?;
            if let Some(store) = &self.store {
                store.on_compensation_started(saga_id, failed_step).await?;
            }
            Ok(())
        })
    }

    fn on_compensated<'a>(
        &'a self,
        saga_id: &'a [u8],
        compensated_steps: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta.on_compensated(saga_id, compensated_steps).await?;
            if let Some(store) = &self.store {
                store.on_compensated(saga_id, compensated_steps).await?;
            }
            Ok(())
        })
    }

    fn on_stuck<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
        compensate_failed_at: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta
                .on_stuck(saga_id, failed_step, compensate_failed_at)
                .await?;
            if let Some(store) = &self.store {
                store
                    .on_stuck(saga_id, failed_step, compensate_failed_at)
                    .await?;
            }
            Ok(())
        })
    }

    fn load<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<SagaJournalRecord>, SagaJournalError>> + Send + 'a>,
    > {
        Box::pin(async move {
            if let Some(store) = &self.store
                && let Some(rec) = store.load(saga_id).await?
            {
                return Ok(Some(rec));
            }
            self.meta.load(saga_id).await
        })
    }
}

/// Back-compat alias — saga journal now replicates via Meta-Raft in multi-Raft mode.
pub type Group0SagaJournal = MetaRaftSagaJournal;

/// Increment Prometheus counters for [`SagaEvent`] (ADR Phase 4).
pub fn record_saga_metrics(metrics: &Metrics, node_id: u64, event: SagaEvent) {
    let node = node_id.to_string();
    let labels = [("node", node.as_str())];
    match event {
        SagaEvent::Completed { .. } => {
            metrics.incr(
                "trembita_saga_completed_total",
                "Cross-shard sagas completed (all forward steps committed)",
                &labels,
                1.0,
            );
        }
        SagaEvent::Compensated { .. } => {
            metrics.incr(
                "trembita_saga_compensated_total",
                "Cross-shard sagas compensated after forward failure",
                &labels,
                1.0,
            );
        }
        SagaEvent::Stuck { .. } => {
            metrics.incr(
                "trembita_saga_stuck_total",
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

/// Metrics hook suitable for [`trembita_client::RunSagaOpts::on_event`].
#[must_use]
pub fn saga_metrics_callback(
    metrics: Metrics,
    node_id: u64,
) -> Arc<dyn Fn(SagaEvent) + Send + Sync> {
    Arc::new(move |event| record_saga_event(&metrics, node_id, event))
}
