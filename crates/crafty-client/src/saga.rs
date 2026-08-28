//! Cross-shard saga coordinator (Tier 2 Phase 4 — framework compensation).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crafty_proto::{decode, encode};
use serde::{Deserialize, Serialize};

use crate::{ClientError, KeyedClient};

/// One forward step with a compensating keyed write on the same shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaStep {
    /// Shard routing key (same shard for forward + compensate).
    pub key: Vec<u8>,
    /// Application-encoded forward command.
    pub command: Vec<u8>,
    /// Application-encoded compensate command (must be idempotent).
    pub compensate: Vec<u8>,
}

/// Ordered cross-shard write plan executed by [`run_saga`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaPlan {
    /// Unique saga id (journal key / idempotency scope).
    pub saga_id: Vec<u8>,
    /// Steps executed in order; compensators run in reverse on forward failure.
    pub steps: Vec<SagaStep>,
}

/// Outcome of a saga run.
#[derive(Debug)]
pub enum SagaOutcome {
    /// Every forward step committed.
    Completed(Vec<Vec<u8>>),
    /// Forward failed at `failed_step`; compensators ran for `[0, failed_step)`.
    Compensated {
        /// Responses from successful forward steps before the failure.
        forward_responses: Vec<Vec<u8>>,
        /// Index of the step that failed.
        failed_step: usize,
        /// How many compensate commands succeeded (reverse order).
        compensated_steps: usize,
        /// Why the forward step failed.
        forward_error: ClientError,
    },
}

/// Why a saga could not finish cleanly.
#[derive(Debug, thiserror::Error)]
pub enum SagaError {
    /// A compensate command failed after partial forward progress.
    #[error(
        "forward failed at step {failed_step} ({forward_completed} committed); \
         compensation failed at step {compensate_failed_at}: {source}"
    )]
    CompensationFailed {
        /// Forward step that failed.
        failed_step: usize,
        /// Forward steps that committed before the failure.
        forward_completed: usize,
        /// Compensate step index that failed.
        compensate_failed_at: usize,
        /// Forward responses collected before compensation.
        forward_responses: Vec<Vec<u8>>,
        #[source]
        /// Client error from the failed compensate RPC.
        source: ClientError,
    },
    /// Journal persistence failed (saga aborted before mutating client state further).
    #[error("saga journal error: {0}")]
    Journal(#[from] SagaJournalError),
    /// [`resume_saga`] found no record for `plan.saga_id`.
    #[error("no journal record for this saga")]
    NotFound,
    /// Catalog generation changed mid-saga (dynamic catalog expansion).
    #[error("catalog version changed during saga (pinned {pinned}, current {current})")]
    CatalogVersionChanged {
        /// Catalog version pinned when the saga started.
        pinned: u32,
        /// Current catalog version observed mid-run.
        current: u32,
    },
}

/// Journal persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum SagaJournalError {
    /// Encode/decode of journal records failed.
    #[error("codec: {0}")]
    Codec(String),
    /// Backend refused the write.
    #[error("backend: {0}")]
    Backend(String),
}

/// Lifecycle events for metrics / logging (see cross-shard-transactions ADR).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaEvent {
    /// All forward steps committed.
    Completed {
        /// Number of steps executed.
        steps: usize,
    },
    /// Forward failed and compensators ran successfully.
    Compensated {
        /// Forward steps that committed before failure.
        forward_steps: usize,
        /// Compensate steps that succeeded.
        compensated: usize,
    },
    /// Compensation failed — saga may be stuck; operator intervention required.
    Stuck {
        /// Forward step that failed.
        failed_step: usize,
        /// Compensate step that failed.
        compensate_failed_at: usize,
    },
}

/// Optional hooks for [`run_saga`].
#[derive(Default)]
pub struct RunSagaOpts<'a> {
    /// Durable (or in-memory) saga journal.
    pub journal: Option<&'a dyn SagaJournal>,
    /// Pin catalog version in the journal (dynamic catalog mid-saga).
    pub catalog_version: Option<u32>,
    /// Live catalog generation checked before each forward step.
    pub catalog_version_live: Option<Arc<AtomicU32>>,
    /// Metrics / logging callback.
    pub on_event: Option<&'a (dyn Fn(SagaEvent) + Send + Sync)>,
}

/// Hooks for [`resume_saga`] (journal required).
pub struct ResumeSagaOpts<'a> {
    /// Durable saga journal to load progress from.
    pub journal: &'a dyn SagaJournal,
    /// Live catalog generation checked before each resumed forward step.
    pub catalog_version_live: Option<Arc<AtomicU32>>,
    /// Metrics / logging callback.
    pub on_event: Option<&'a (dyn Fn(SagaEvent) + Send + Sync)>,
}

/// Durable saga progress (journal value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaJournalRecord {
    /// Saga identifier.
    pub saga_id: Vec<u8>,
    /// Latest phase.
    pub phase: SagaJournalPhase,
    /// Forward steps committed so far.
    pub completed_steps: u32,
    /// Catalog version pinned at start, if any.
    pub catalog_version: Option<u32>,
    /// Forward step that failed before compensation (if any).
    #[serde(default)]
    pub failed_step: Option<u32>,
    /// Compensate step index that failed when phase is [`SagaJournalPhase::Stuck`].
    #[serde(default)]
    pub compensate_failed_at: Option<u32>,
}

/// Journal phase for resume / observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SagaJournalPhase {
    /// Forward execution in progress.
    Running,
    /// All forward steps committed.
    Completed,
    /// Running compensators after a forward failure.
    Compensating,
    /// Compensators finished (success or partial — see `completed_steps`).
    Compensated,
    /// Compensation could not complete.
    Stuck,
}

/// Object-safe saga journal (Redis, group-0 side channel, in-memory tests).
pub trait SagaJournal: Send + Sync {
    /// Persist saga start.
    fn on_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        steps: usize,
        catalog_version: Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>>;

    /// Persist a committed forward step.
    fn on_step_committed<'a>(
        &'a self,
        saga_id: &'a [u8],
        step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>>;

    /// Persist saga completion.
    fn on_completed<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>>;

    /// Persist compensation start after forward failure.
    fn on_compensation_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>>;

    /// Persist successful compensation.
    fn on_compensated<'a>(
        &'a self,
        saga_id: &'a [u8],
        compensated_steps: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>>;

    /// Persist stuck saga (compensation failure).
    fn on_stuck<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
        compensate_failed_at: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>>;

    /// Load the latest journal record for `saga_id`, if any.
    fn load<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<SagaJournalRecord>, SagaJournalError>> + Send + 'a>,
    >;
}

/// In-memory journal for unit tests.
#[derive(Default)]
pub struct InMemorySagaJournal {
    records: Mutex<Vec<SagaJournalRecord>>,
}

impl InMemorySagaJournal {
    /// Snapshot persisted records.
    ///
    /// # Panics
    /// Panics if the journal lock is poisoned.
    #[must_use]
    pub fn records(&self) -> Vec<SagaJournalRecord> {
        self.records.lock().expect("lock").clone()
    }

    fn upsert(&self, saga_id: &[u8], f: impl FnOnce(&mut SagaJournalRecord)) {
        let mut guard = self.records.lock().expect("lock");
        if let Some(rec) = guard.iter_mut().find(|r| r.saga_id == saga_id) {
            f(rec);
            return;
        }
        let mut rec = SagaJournalRecord {
            saga_id: saga_id.to_vec(),
            phase: SagaJournalPhase::Running,
            completed_steps: 0,
            catalog_version: None,
            failed_step: None,
            compensate_failed_at: None,
        };
        f(&mut rec);
        guard.push(rec);
    }
}

impl SagaJournal for InMemorySagaJournal {
    fn on_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        _steps: usize,
        catalog_version: Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(saga_id, |rec| {
                rec.phase = SagaJournalPhase::Running;
                rec.completed_steps = 0;
                rec.catalog_version = catalog_version;
                rec.failed_step = None;
                rec.compensate_failed_at = None;
            });
            Ok(())
        })
    }

    fn on_step_committed<'a>(
        &'a self,
        saga_id: &'a [u8],
        step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(saga_id, |rec| {
                rec.completed_steps = u32::try_from(step).expect("step index fits u32") + 1;
            });
            Ok(())
        })
    }

    fn on_completed<'a>(
        &'a self,
        saga_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(saga_id, |rec| rec.phase = SagaJournalPhase::Completed);
            Ok(())
        })
    }

    fn on_compensation_started<'a>(
        &'a self,
        saga_id: &'a [u8],
        failed_step: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(saga_id, |rec| {
                rec.phase = SagaJournalPhase::Compensating;
                rec.failed_step = Some(u32::try_from(failed_step).expect("step index fits u32"));
            });
            Ok(())
        })
    }

    fn on_compensated<'a>(
        &'a self,
        saga_id: &'a [u8],
        _compensated_steps: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), SagaJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(saga_id, |rec| {
                rec.phase = SagaJournalPhase::Compensated;
                rec.compensate_failed_at = None;
            });
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
            self.upsert(saga_id, |rec| {
                rec.phase = SagaJournalPhase::Stuck;
                rec.failed_step = Some(u32::try_from(failed_step).expect("step index fits u32"));
                rec.compensate_failed_at =
                    Some(u32::try_from(compensate_failed_at).expect("step index fits u32"));
            });
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
            let guard = self.records.lock().expect("lock");
            Ok(guard.iter().find(|r| r.saga_id == saga_id).cloned())
        })
    }
}

/// Encode a [`SagaJournalRecord`] for external stores.
///
/// # Errors
/// Returns [`SagaJournalError::Codec`] when encoding fails.
pub fn encode_journal_record(record: &SagaJournalRecord) -> Result<Vec<u8>, SagaJournalError> {
    encode(record).map_err(|e| SagaJournalError::Codec(e.to_string()))
}

/// Decode a [`SagaJournalRecord`].
///
/// # Errors
/// Returns [`SagaJournalError::Codec`] when decoding fails.
pub fn decode_journal_record(bytes: &[u8]) -> Result<SagaJournalRecord, SagaJournalError> {
    decode(bytes).map_err(|e| SagaJournalError::Codec(e.to_string()))
}

fn check_catalog_version(opts: &RunSagaOpts<'_>) -> Result<(), SagaError> {
    if let (Some(pinned), Some(live)) = (opts.catalog_version, &opts.catalog_version_live) {
        let current = live.load(Ordering::SeqCst);
        if current != pinned {
            return Err(SagaError::CatalogVersionChanged { pinned, current });
        }
    }
    Ok(())
}

async fn run_forward<C: KeyedClient>(
    client: &C,
    plan: &SagaPlan,
    opts: &RunSagaOpts<'_>,
    start_step: usize,
    mut responses: Vec<Vec<u8>>,
) -> Result<(Vec<Vec<u8>>, Option<(usize, ClientError)>), SagaError> {
    for (step, item) in plan.steps.iter().enumerate().skip(start_step) {
        check_catalog_version(opts)?;
        match client
            .propose_keyed(item.key.clone(), item.command.clone())
            .await
        {
            Ok(bytes) => {
                responses.push(bytes);
                if let Some(journal) = opts.journal {
                    journal.on_step_committed(&plan.saga_id, step).await?;
                }
            }
            Err(forward_error) => {
                return Ok((responses, Some((step, forward_error))));
            }
        }
    }
    Ok((responses, None))
}

#[allow(clippy::too_many_arguments)]
async fn run_compensation<C: KeyedClient>(
    client: &C,
    plan: &SagaPlan,
    opts: &RunSagaOpts<'_>,
    failed_step: usize,
    forward_responses: Vec<Vec<u8>>,
    forward_error: ClientError,
    from_rev: usize,
    record_compensation_start: bool,
) -> Result<SagaOutcome, SagaError> {
    if record_compensation_start && let Some(journal) = opts.journal {
        journal
            .on_compensation_started(&plan.saga_id, failed_step)
            .await?;
    }

    let mut compensated_steps = 0usize;
    if failed_step == 0 {
        if let Some(journal) = opts.journal {
            journal.on_compensated(&plan.saga_id, 0).await?;
        }
        if let Some(on) = opts.on_event {
            on(SagaEvent::Compensated {
                forward_steps: 0,
                compensated: 0,
            });
        }
        return Ok(SagaOutcome::Compensated {
            forward_responses,
            failed_step: 0,
            compensated_steps: 0,
            forward_error,
        });
    }

    for rev in (0..=from_rev).rev() {
        let back = &plan.steps[rev];
        if let Err(source) = client
            .propose_keyed(back.key.clone(), back.compensate.clone())
            .await
        {
            if let Some(journal) = opts.journal {
                journal.on_stuck(&plan.saga_id, failed_step, rev).await?;
            }
            if let Some(on) = opts.on_event {
                on(SagaEvent::Stuck {
                    failed_step,
                    compensate_failed_at: rev,
                });
            }
            return Err(SagaError::CompensationFailed {
                failed_step,
                forward_completed: failed_step,
                compensate_failed_at: rev,
                forward_responses,
                source,
            });
        }
        compensated_steps += 1;
    }

    if let Some(journal) = opts.journal {
        journal
            .on_compensated(&plan.saga_id, compensated_steps)
            .await?;
    }
    if let Some(on) = opts.on_event {
        on(SagaEvent::Compensated {
            forward_steps: failed_step,
            compensated: compensated_steps,
        });
    }
    Ok(SagaOutcome::Compensated {
        forward_responses,
        failed_step,
        compensated_steps,
        forward_error,
    })
}

fn run_opts_from_record<'a>(
    journal: &'a dyn SagaJournal,
    record: &SagaJournalRecord,
    catalog_version_live: Option<Arc<AtomicU32>>,
    on_event: Option<&'a (dyn Fn(SagaEvent) + Send + Sync)>,
) -> RunSagaOpts<'a> {
    RunSagaOpts {
        journal: Some(journal),
        catalog_version: record.catalog_version,
        catalog_version_live,
        on_event,
    }
}

/// Execute `plan` forward; on failure run compensators in reverse for committed steps.
///
/// **Not** serializable atomicity — see `docs/decisions/multi-raft.md#cross-shard-transactions`.
///
/// # Errors
/// [`SagaError::CompensationFailed`] when a compensate command fails.
/// [`SagaError::Journal`] when the journal hook returns an error.
pub async fn run_saga<C: KeyedClient>(
    client: &C,
    plan: &SagaPlan,
    opts: RunSagaOpts<'_>,
) -> Result<SagaOutcome, SagaError> {
    if plan.steps.is_empty() {
        if let Some(on) = opts.on_event {
            on(SagaEvent::Completed { steps: 0 });
        }
        return Ok(SagaOutcome::Completed(Vec::new()));
    }

    if let Some(journal) = opts.journal {
        if let Some(record) = journal.load(&plan.saga_id).await?
            && record.phase == SagaJournalPhase::Completed
        {
            if let Some(on) = opts.on_event {
                on(SagaEvent::Completed {
                    steps: plan.steps.len(),
                });
            }
            return Ok(SagaOutcome::Completed(Vec::new()));
        }

        journal
            .on_started(&plan.saga_id, plan.steps.len(), opts.catalog_version)
            .await?;
    }

    let (responses, failure) = run_forward(client, plan, &opts, 0, Vec::new()).await?;
    if let Some((failed_step, forward_error)) = failure {
        let from_rev = failed_step.saturating_sub(1);
        return run_compensation(
            client,
            plan,
            &opts,
            failed_step,
            responses,
            forward_error,
            from_rev,
            true,
        )
        .await;
    }

    if let Some(journal) = opts.journal {
        journal.on_completed(&plan.saga_id).await?;
    }
    if let Some(on) = opts.on_event {
        on(SagaEvent::Completed {
            steps: plan.steps.len(),
        });
    }
    Ok(SagaOutcome::Completed(responses))
}

/// Continue a saga from its durable journal record.
///
/// # Errors
/// [`SagaError::NotFound`] when no journal record exists.
/// Same errors as [`run_saga`] for forward/compensation failures.
pub async fn resume_saga<C: KeyedClient>(
    client: &C,
    plan: &SagaPlan,
    opts: ResumeSagaOpts<'_>,
) -> Result<SagaOutcome, SagaError> {
    let Some(record) = opts.journal.load(&plan.saga_id).await? else {
        return Err(SagaError::NotFound);
    };

    let run_opts = run_opts_from_record(
        opts.journal,
        &record,
        opts.catalog_version_live.clone(),
        opts.on_event,
    );

    match record.phase {
        SagaJournalPhase::Completed => {
            if let Some(on) = opts.on_event {
                on(SagaEvent::Completed {
                    steps: plan.steps.len(),
                });
            }
            Ok(SagaOutcome::Completed(Vec::new()))
        }
        SagaJournalPhase::Compensated => Ok(SagaOutcome::Compensated {
            forward_responses: Vec::new(),
            failed_step: record.failed_step.unwrap_or(0) as usize,
            compensated_steps: 0,
            forward_error: ClientError::Server("resumed compensated saga".into()),
        }),
        SagaJournalPhase::Running => {
            let start = record.completed_steps as usize;
            let (responses, failure) =
                run_forward(client, plan, &run_opts, start, Vec::new()).await?;
            if let Some((failed_step, forward_error)) = failure {
                let from_rev = failed_step.saturating_sub(1);
                return run_compensation(
                    client,
                    plan,
                    &run_opts,
                    failed_step,
                    responses,
                    forward_error,
                    from_rev,
                    true,
                )
                .await;
            }
            if let Some(journal) = run_opts.journal {
                journal.on_completed(&plan.saga_id).await?;
            }
            if let Some(on) = opts.on_event {
                on(SagaEvent::Completed {
                    steps: plan.steps.len(),
                });
            }
            Ok(SagaOutcome::Completed(responses))
        }
        SagaJournalPhase::Compensating | SagaJournalPhase::Stuck => {
            let failed_step = record.failed_step.ok_or(SagaError::NotFound)? as usize;
            let from_rev = match record.phase {
                SagaJournalPhase::Stuck => {
                    record.compensate_failed_at.ok_or(SagaError::NotFound)? as usize
                }
                _ => failed_step.saturating_sub(1),
            };
            run_compensation(
                client,
                plan,
                &run_opts,
                failed_step,
                Vec::new(),
                ClientError::Server("resumed compensation".into()),
                from_rev,
                record.phase == SagaJournalPhase::Compensating,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crafty_net::{Route, Transport, TransportError, decode_body, encode_body};
    use crafty_proto::{ClientRequest, ClientResponse, NodeId};

    use super::*;
    use crate::{RemoteClient, RetryPolicy};

    struct SagaScript {
        forward_ok: u32,
        compensate_ok: u32,
        forward_calls: Arc<AtomicU32>,
        compensate_calls: Arc<AtomicU32>,
    }

    impl Transport for SagaScript {
        fn send(
            &self,
            _peer: NodeId,
            _route: Route,
            body: crafty_net::transport::Body,
        ) -> crafty_net::transport::BoxFuture<
            'static,
            Result<crafty_net::transport::Body, TransportError>,
        > {
            let request = match decode_body::<ClientRequest>(&body) {
                Ok(r) => r,
                Err(e) => {
                    return Box::pin(async move { Err(TransportError::Wire(e)) });
                }
            };
            let forward_ok = self.forward_ok;
            let compensate_ok = self.compensate_ok;
            let forward_calls = Arc::clone(&self.forward_calls);
            let compensate_calls = Arc::clone(&self.compensate_calls);
            Box::pin(async move {
                match request {
                    ClientRequest::ProposeKeyed { command, .. } => {
                        if command.first() == Some(&0xFF) {
                            let n = compensate_calls.fetch_add(1, Ordering::Relaxed);
                            if n >= compensate_ok {
                                return Err(TransportError::Unreachable(NodeId(1)));
                            }
                        } else {
                            let n = forward_calls.fetch_add(1, Ordering::Relaxed);
                            if n >= forward_ok {
                                return Err(TransportError::Unreachable(NodeId(1)));
                            }
                        }
                        encode_body(&ClientResponse::Ok(b"ok".to_vec()))
                            .map_err(TransportError::Wire)
                    }
                    _ => Err(TransportError::Io("unexpected request".into())),
                }
            })
        }
    }

    fn client(script: Arc<SagaScript>) -> RemoteClient {
        RemoteClient::new(script, [NodeId(1)]).with_retry(RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        })
    }

    fn two_step_plan() -> SagaPlan {
        SagaPlan {
            saga_id: b"transfer-1".to_vec(),
            steps: vec![
                SagaStep {
                    key: b"shard-a".to_vec(),
                    command: vec![1],
                    compensate: vec![0xFF, 1],
                },
                SagaStep {
                    key: b"shard-b".to_vec(),
                    command: vec![2],
                    compensate: vec![0xFF, 2],
                },
            ],
        }
    }

    #[tokio::test]
    async fn saga_completes_all_forward_steps() {
        let script = Arc::new(SagaScript {
            forward_ok: 2,
            compensate_ok: 0,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let journal = InMemorySagaJournal::default();
        let outcome = run_saga(
            &client,
            &two_step_plan(),
            RunSagaOpts {
                journal: Some(&journal),
                ..RunSagaOpts::default()
            },
        )
        .await
        .expect("saga completes");
        assert!(matches!(outcome, SagaOutcome::Completed(_)));
        assert_eq!(script.compensate_calls.load(Ordering::Relaxed), 0);
        assert!(
            journal
                .records()
                .iter()
                .any(|r| r.phase == SagaJournalPhase::Completed)
        );
    }

    #[tokio::test]
    async fn saga_compensates_after_second_forward_fails() {
        let script = Arc::new(SagaScript {
            forward_ok: 1,
            compensate_ok: 1,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let journal = InMemorySagaJournal::default();
        let outcome = run_saga(
            &client,
            &two_step_plan(),
            RunSagaOpts {
                journal: Some(&journal),
                ..RunSagaOpts::default()
            },
        )
        .await
        .expect("compensated saga");
        let SagaOutcome::Compensated {
            failed_step,
            compensated_steps,
            ..
        } = outcome
        else {
            panic!("expected compensated, got {outcome:?}");
        };
        assert_eq!(failed_step, 1);
        assert_eq!(compensated_steps, 1);
        assert_eq!(script.compensate_calls.load(Ordering::Relaxed), 1);
        assert!(
            journal
                .records()
                .iter()
                .any(|r| r.phase == SagaJournalPhase::Compensated)
        );
    }

    #[tokio::test]
    async fn saga_stuck_when_compensation_fails() {
        let script = Arc::new(SagaScript {
            forward_ok: 1,
            compensate_ok: 0,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let err = run_saga(&client, &two_step_plan(), RunSagaOpts::default())
            .await
            .unwrap_err();
        assert!(matches!(err, SagaError::CompensationFailed { .. }));
    }

    #[tokio::test]
    async fn resume_saga_continues_forward_after_partial_journal() {
        let script = Arc::new(SagaScript {
            forward_ok: 2,
            compensate_ok: 0,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let journal = InMemorySagaJournal::default();
        let plan = two_step_plan();
        journal
            .on_started(&plan.saga_id, plan.steps.len(), None)
            .await
            .expect("seed");
        journal
            .on_step_committed(&plan.saga_id, 0)
            .await
            .expect("seed step");

        let outcome = resume_saga(
            &client,
            &plan,
            ResumeSagaOpts {
                journal: &journal,
                catalog_version_live: None,
                on_event: None,
            },
        )
        .await
        .expect("resume completes");
        assert!(matches!(outcome, SagaOutcome::Completed(_)));
        assert_eq!(script.forward_calls.load(Ordering::Relaxed), 1);
        assert!(
            journal
                .records()
                .iter()
                .any(|r| r.phase == SagaJournalPhase::Completed)
        );
    }

    #[tokio::test]
    async fn resume_saga_retries_stuck_compensation() {
        let script = Arc::new(SagaScript {
            forward_ok: 0,
            compensate_ok: 1,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let journal = InMemorySagaJournal::default();
        let plan = two_step_plan();
        journal
            .on_started(&plan.saga_id, plan.steps.len(), None)
            .await
            .expect("seed");
        journal
            .on_step_committed(&plan.saga_id, 0)
            .await
            .expect("seed step");
        journal
            .on_compensation_started(&plan.saga_id, 1)
            .await
            .expect("seed compensating");
        journal
            .on_stuck(&plan.saga_id, 1, 0)
            .await
            .expect("seed stuck");

        let outcome = resume_saga(
            &client,
            &plan,
            ResumeSagaOpts {
                journal: &journal,
                catalog_version_live: None,
                on_event: None,
            },
        )
        .await
        .expect("resume compensation");
        assert!(matches!(outcome, SagaOutcome::Compensated { .. }));
        assert_eq!(script.compensate_calls.load(Ordering::Relaxed), 1);
        assert!(
            journal
                .records()
                .iter()
                .any(|r| r.phase == SagaJournalPhase::Compensated)
        );
    }

    #[tokio::test]
    async fn saga_rejects_catalog_version_change() {
        let script = Arc::new(SagaScript {
            forward_ok: 1,
            compensate_ok: 0,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let journal = InMemorySagaJournal::default();
        let live = Arc::new(AtomicU32::new(2));
        let err = run_saga(
            &client,
            &SagaPlan {
                saga_id: b"transfer-2".to_vec(),
                steps: vec![SagaStep {
                    key: b"shard-a".to_vec(),
                    command: vec![1],
                    compensate: vec![0xFF, 1],
                }],
            },
            RunSagaOpts {
                journal: Some(&journal),
                catalog_version: Some(1),
                catalog_version_live: Some(live),
                ..RunSagaOpts::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            SagaError::CatalogVersionChanged {
                pinned: 1,
                current: 2
            }
        ));
    }

    #[tokio::test]
    async fn saga_is_idempotent_when_journal_completed() {
        let script = Arc::new(SagaScript {
            forward_ok: 0,
            compensate_ok: 0,
            forward_calls: Arc::new(AtomicU32::new(0)),
            compensate_calls: Arc::new(AtomicU32::new(0)),
        });
        let client = client(Arc::clone(&script));
        let journal = InMemorySagaJournal::default();
        journal.on_started(b"done", 1, Some(1)).await.expect("seed");
        journal.on_completed(b"done").await.expect("seed complete");

        let outcome = run_saga(
            &client,
            &SagaPlan {
                saga_id: b"done".to_vec(),
                steps: vec![SagaStep {
                    key: b"k".to_vec(),
                    command: vec![1],
                    compensate: vec![0xFF],
                }],
            },
            RunSagaOpts {
                journal: Some(&journal),
                ..RunSagaOpts::default()
            },
        )
        .await
        .expect("idempotent replay");
        assert!(matches!(outcome, SagaOutcome::Completed(_)));
        assert_eq!(script.forward_calls.load(Ordering::Relaxed), 0);
    }
}
