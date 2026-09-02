//! 2PC client journal backed by Meta-Raft metadata and/or [`ActorStateStore`].

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use trembita_actor::{ActorStateStore, NodeHandle};
use trembita_client::{
    TwoPhaseEvent, TwoPhaseJournal, TwoPhaseJournalError, TwoPhaseJournalRecord,
    decode_two_phase_journal_record, encode_two_phase_journal_record,
};
use trembita_core::StateMachine;
use trembita_dashboard::Metrics;
use trembita_proto::TwoPhaseJournalCommand;

fn fresh_record(tx_id: &[u8]) -> TwoPhaseJournalRecord {
    TwoPhaseJournalRecord {
        tx_id: tx_id.to_vec(),
        prepared_steps: 0,
        committed_steps: 0,
    }
}

fn journal_key(tx_id: &[u8]) -> String {
    format!("trembita:2pc:{}", String::from_utf8_lossy(tx_id))
}

/// Persist 2PC coordinator progress in an external workflow store (Redis / in-memory).
pub struct StoreTwoPhaseJournal {
    store: Arc<dyn ActorStateStore>,
}

impl StoreTwoPhaseJournal {
    /// Wrap `store` for 2PC client journaling.
    #[must_use]
    pub fn new(store: Arc<dyn ActorStateStore>) -> Self {
        Self { store }
    }

    async fn read(
        &self,
        tx_id: &[u8],
    ) -> Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError> {
        let Some(bytes) = self
            .store
            .get(&journal_key(tx_id))
            .await
            .map_err(|e| TwoPhaseJournalError::Backend(e.to_string()))?
        else {
            return Ok(None);
        };
        decode_two_phase_journal_record(&bytes).map(Some)
    }

    async fn update(
        &self,
        tx_id: &[u8],
        f: impl FnOnce(TwoPhaseJournalRecord) -> TwoPhaseJournalRecord,
    ) -> Result<(), TwoPhaseJournalError> {
        let prev = self.read(tx_id).await?;
        let rec = prev.unwrap_or_else(|| fresh_record(tx_id));
        let updated = f(rec);
        let bytes = encode_two_phase_journal_record(&updated)?;
        self.store
            .set(&journal_key(tx_id), &bytes, None)
            .await
            .map_err(|e| TwoPhaseJournalError::Backend(e.to_string()))
    }
}

impl TwoPhaseJournal for StoreTwoPhaseJournal {
    fn on_prepared<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        _total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(tx_id, |mut rec| {
                rec.prepared_steps =
                    (u32::try_from(step).expect("step index fits u32") + 1).max(rec.prepared_steps);
                rec
            })
            .await
        })
    }

    fn on_committed<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        _total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(tx_id, |mut rec| {
                rec.committed_steps = (u32::try_from(step).expect("step index fits u32") + 1)
                    .max(rec.committed_steps);
                rec
            })
            .await
        })
    }

    fn on_completed<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(tx_id, |mut rec| {
                rec.committed_steps = rec.prepared_steps;
                rec
            })
            .await
        })
    }

    fn load<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { self.read(tx_id).await })
    }
}

/// In-memory view of 2PC client journal records applied from Meta-Raft (all replicas).
pub type TwoPhaseRegistry = Arc<Mutex<BTreeMap<Vec<u8>, TwoPhaseJournalRecord>>>;

type TwoPhaseJournalUpsertFn = dyn Fn(
        TwoPhaseJournalCommand,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send>>
    + Send
    + Sync;

/// Persist 2PC client progress in Meta-Raft coordinator metadata (no Redis required).
pub struct MetaRaftTwoPhaseJournal {
    upsert: Arc<TwoPhaseJournalUpsertFn>,
    registry: TwoPhaseRegistry,
}

impl MetaRaftTwoPhaseJournal {
    /// Build a journal that proposes upserts on the Meta-Raft group and reads `registry`.
    #[must_use]
    pub fn new<M: StateMachine + 'static>(meta: NodeHandle<M>, registry: TwoPhaseRegistry) -> Self {
        let upsert = Arc::new(move |command: TwoPhaseJournalCommand| {
            let meta = meta.clone();
            Box::pin(async move {
                meta.upsert_two_phase_journal(command)
                    .await
                    .map_err(|e| TwoPhaseJournalError::Backend(e.to_string()))
            }) as Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send>>
        });
        Self { upsert, registry }
    }

    fn read(&self, tx_id: &[u8]) -> Option<TwoPhaseJournalRecord> {
        self.registry.lock().expect("lock").get(tx_id).cloned()
    }

    async fn update(
        &self,
        tx_id: &[u8],
        f: impl FnOnce(TwoPhaseJournalRecord) -> TwoPhaseJournalRecord,
    ) -> Result<(), TwoPhaseJournalError> {
        let prev = self.read(tx_id);
        let updated = f(prev.unwrap_or_else(|| fresh_record(tx_id)));
        let command = TwoPhaseJournalCommand {
            tx_id: tx_id.to_vec(),
            record: encode_two_phase_journal_record(&updated)?,
        };
        (self.upsert)(command).await
    }
}

impl TwoPhaseJournal for MetaRaftTwoPhaseJournal {
    fn on_prepared<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(tx_id, |mut rec| {
                rec.prepared_steps =
                    (u32::try_from(step).expect("step index fits u32") + 1).max(rec.prepared_steps);
                let _ = total;
                rec
            })
            .await
        })
    }

    fn on_committed<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(tx_id, |mut rec| {
                rec.committed_steps = (u32::try_from(step).expect("step index fits u32") + 1)
                    .max(rec.committed_steps);
                let _ = total;
                rec
            })
            .await
        })
    }

    fn on_completed<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.update(tx_id, |mut rec| {
                rec.committed_steps = rec.prepared_steps;
                rec
            })
            .await
        })
    }

    fn load<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { Ok(self.read(tx_id)) })
    }
}

/// Replicate to Meta-Raft and optionally mirror in an external store (Redis).
pub struct CompositeTwoPhaseJournal {
    meta: MetaRaftTwoPhaseJournal,
    store: Option<StoreTwoPhaseJournal>,
}

impl CompositeTwoPhaseJournal {
    /// Meta-Raft is always the durable fallback; `store` is an optional mirror.
    #[must_use]
    pub fn new(meta: MetaRaftTwoPhaseJournal, store: Option<StoreTwoPhaseJournal>) -> Self {
        Self { meta, store }
    }
}

impl TwoPhaseJournal for CompositeTwoPhaseJournal {
    fn on_prepared<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta.on_prepared(tx_id, step, total).await?;
            if let Some(store) = &self.store {
                store.on_prepared(tx_id, step, total).await?;
            }
            Ok(())
        })
    }

    fn on_committed<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta.on_committed(tx_id, step, total).await?;
            if let Some(store) = &self.store {
                store.on_committed(tx_id, step, total).await?;
            }
            Ok(())
        })
    }

    fn on_completed<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.meta.on_completed(tx_id).await?;
            if let Some(store) = &self.store {
                store.on_completed(tx_id).await?;
            }
            Ok(())
        })
    }

    fn load<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if let Some(store) = &self.store
                && let Some(rec) = store.load(tx_id).await?
            {
                return Ok(Some(rec));
            }
            self.meta.load(tx_id).await
        })
    }
}

/// Increment Prometheus counters for [`TwoPhaseEvent`] and server-side GC.
pub fn record_two_phase_metrics(metrics: &Metrics, node_id: u64, event: TwoPhaseEvent) {
    let node = node_id.to_string();
    let labels = [("node", node.as_str())];
    match event {
        TwoPhaseEvent::Prepared { .. } => {
            metrics.incr(
                "trembita_2pc_prepared_total",
                "Cross-shard 2PC transactions that completed the prepare phase",
                &labels,
                1.0,
            );
        }
        TwoPhaseEvent::Stuck { .. } => {
            metrics.incr(
                "trembita_2pc_stuck_total",
                "Cross-shard 2PC coordinators stuck after partial progress",
                &labels,
                1.0,
            );
        }
    }
}

/// Record one server-side GC abort on this node's metrics registry.
pub fn record_two_phase_gc_aborted(metrics: &Metrics, node_id: u64) {
    let node = node_id.to_string();
    let labels = [("node", node.as_str())];
    metrics.incr(
        "trembita_2pc_gc_aborted_total",
        "Durable 2PC prepares aborted by leader timeout GC",
        &labels,
        1.0,
    );
}

/// Record one 2PC lifecycle event on the cluster metrics registry.
pub fn record_two_phase_event(metrics: &Metrics, node_id: u64, event: TwoPhaseEvent) {
    record_two_phase_metrics(metrics, node_id, event);
}

/// Metrics hook suitable for [`trembita_client::RunTwoPhaseOpts::on_event`].
#[must_use]
pub fn two_phase_metrics_callback(
    metrics: Metrics,
    node_id: u64,
) -> Arc<dyn Fn(TwoPhaseEvent) + Send + Sync> {
    Arc::new(move |event| record_two_phase_event(&metrics, node_id, event))
}
