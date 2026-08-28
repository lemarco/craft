//! Cross-shard two-phase commit coordinator (optional Tier 2 increment).

use std::future::Future;
use std::pin::Pin;

use crafty_core::{TwoPhasePlan, TwoPhasePlanError, validate_two_phase_plan};

use crate::{ClientError, KeyedClient};

/// Extension of [`KeyedClient`] for limited cross-shard 2PC.
pub trait TwoPhaseClient: KeyedClient {
    /// Stage a command on the shard for `key` under `tx_id`.
    fn prepare_keyed(
        &self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
        command: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;

    /// Commit a previously prepared command.
    fn commit_keyed(
        &self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;

    /// Abort a prepared command and release staging state.
    fn abort_keyed(
        &self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;
}

/// Why a cross-shard 2PC attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum TwoPhaseError {
    /// The coordinator plan was invalid (duplicate keys, empty steps, etc.).
    #[error("invalid 2PC plan: {0}")]
    Plan(#[from] TwoPhasePlanError),
    /// Durable journal read/write failed before or during the transaction.
    #[error("2PC journal error: {0}")]
    Journal(#[from] TwoPhaseJournalError),
    /// A prepare RPC failed after earlier steps succeeded.
    #[error("2PC prepare failed at step {step} after {prepared} prepare(s): {source}")]
    Prepare {
        /// Zero-based step index that failed.
        step: usize,
        /// Prepare steps that succeeded before the failure.
        prepared: usize,
        #[source]
        /// Underlying client / transport error.
        source: ClientError,
    },
    /// A commit RPC failed after earlier steps succeeded.
    #[error("2PC commit failed at step {step} after {committed} commit(s): {source}")]
    Commit {
        /// Zero-based step index that failed.
        step: usize,
        /// Commit steps that succeeded before the failure.
        committed: usize,
        #[source]
        /// Underlying client / transport error.
        source: ClientError,
    },
}

/// Journal persistence failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TwoPhaseJournalError {
    /// Journal record encode/decode failed.
    #[error("journal codec error: {0}")]
    Codec(String),
    /// Underlying journal storage backend failed.
    #[error("journal backend error: {0}")]
    Backend(String),
}

/// Lifecycle events for metrics / logging (see cross-shard-transactions ADR).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoPhaseEvent {
    /// All prepare steps succeeded (ready for commit phase).
    Prepared {
        /// Number of prepared steps.
        steps: usize,
    },
    /// Coordinator stuck after partial progress (abort or commit failed).
    Stuck {
        /// Steps prepared before failure.
        prepared: usize,
        /// Step index where failure occurred.
        failed_step: usize,
    },
}

/// Client-side durable progress for cross-shard 2PC resume.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TwoPhaseJournalRecord {
    /// Shared transaction id (matches [`TwoPhasePlan::tx_id`]).
    pub tx_id: Vec<u8>,
    /// Count of consecutive prepared steps from step 0.
    pub prepared_steps: u32,
    /// Count of consecutive committed steps from step 0.
    pub committed_steps: u32,
}

/// Optional journal hook for [`propose_cross_shard_2pc`] / [`resume_cross_shard_2pc`].
pub trait TwoPhaseJournal: Send + Sync {
    /// Persist progress after a successful prepare step.
    fn on_prepared<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>>;

    /// Persist progress after a successful commit step.
    fn on_committed<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>>;

    /// Mark the transaction fully committed (may delete the journal row).
    fn on_completed<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>>;

    /// Load coordinator progress for resume.
    fn load<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError>>
                + Send
                + 'a,
        >,
    >;
}

/// In-memory 2PC journal (tests and single-process coordinators).
#[derive(Debug, Default)]
pub struct InMemoryTwoPhaseJournal {
    records: std::sync::Mutex<Vec<TwoPhaseJournalRecord>>,
}

impl InMemoryTwoPhaseJournal {
    /// Snapshot persisted journal records.
    ///
    /// # Panics
    /// Panics if the journal lock is poisoned.
    #[must_use]
    pub fn records(&self) -> Vec<TwoPhaseJournalRecord> {
        self.records.lock().expect("lock").clone()
    }

    fn upsert(&self, tx_id: &[u8], f: impl FnOnce(&mut TwoPhaseJournalRecord)) {
        let mut guard = self.records.lock().expect("lock");
        if let Some(rec) = guard.iter_mut().find(|r| r.tx_id == tx_id) {
            f(rec);
            return;
        }
        let mut rec = TwoPhaseJournalRecord {
            tx_id: tx_id.to_vec(),
            prepared_steps: 0,
            committed_steps: 0,
        };
        f(&mut rec);
        guard.push(rec);
    }
}

impl TwoPhaseJournal for InMemoryTwoPhaseJournal {
    fn on_prepared<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        _total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(tx_id, |rec| {
                rec.prepared_steps =
                    (u32::try_from(step).expect("step index fits u32") + 1).max(rec.prepared_steps);
            });
            Ok(())
        })
    }

    fn on_committed<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        _total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(tx_id, |rec| {
                rec.committed_steps = (u32::try_from(step).expect("step index fits u32") + 1)
                    .max(rec.committed_steps);
            });
            Ok(())
        })
    }

    fn on_completed<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert(tx_id, |rec| {
                rec.committed_steps = rec.prepared_steps;
            });
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
            Ok(self
                .records
                .lock()
                .expect("lock")
                .iter()
                .find(|r| r.tx_id == tx_id)
                .cloned())
        })
    }
}

/// Hooks for [`propose_cross_shard_2pc`].
#[derive(Clone, Copy, Default)]
pub struct RunTwoPhaseOpts<'a> {
    /// Optional client-side journal for resume after coordinator restart.
    pub journal: Option<&'a dyn TwoPhaseJournal>,
    /// Metrics / logging callback.
    pub on_event: Option<&'a (dyn Fn(TwoPhaseEvent) + Send + Sync)>,
}

/// Hooks for [`resume_cross_shard_2pc`].
#[derive(Clone, Copy)]
pub struct ResumeTwoPhaseOpts<'a> {
    /// Journal that records prepared/committed prefixes.
    pub journal: Option<&'a dyn TwoPhaseJournal>,
    /// When `true`, try `commit_keyed` before `prepare_keyed` for unknown steps.
    pub probe: bool,
    /// Metrics / logging callback.
    pub on_event: Option<&'a (dyn Fn(TwoPhaseEvent) + Send + Sync)>,
}

impl Default for ResumeTwoPhaseOpts<'_> {
    fn default() -> Self {
        Self {
            journal: None,
            probe: true,
            on_event: None,
        }
    }
}

/// Postcard-encode a [`TwoPhaseJournalRecord`] for Meta-Raft / Redis storage.
///
/// # Errors
/// Returns [`TwoPhaseJournalError::Codec`] when postcard encoding fails.
pub fn encode_two_phase_journal_record(
    record: &TwoPhaseJournalRecord,
) -> Result<Vec<u8>, TwoPhaseJournalError> {
    crafty_proto::encode(record).map_err(|e| TwoPhaseJournalError::Codec(e.to_string()))
}

/// Postcard-decode a [`TwoPhaseJournalRecord`].
///
/// # Errors
/// Returns [`TwoPhaseJournalError::Codec`] when postcard decoding fails.
pub fn decode_two_phase_journal_record(
    bytes: &[u8],
) -> Result<TwoPhaseJournalRecord, TwoPhaseJournalError> {
    crafty_proto::decode(bytes).map_err(|e| TwoPhaseJournalError::Codec(e.to_string()))
}

fn emit_stuck(
    on_event: Option<&(dyn Fn(TwoPhaseEvent) + Send + Sync)>,
    prepared: usize,
    failed_step: usize,
) {
    if let Some(on) = on_event {
        on(TwoPhaseEvent::Stuck {
            prepared,
            failed_step,
        });
    }
}

async fn abort_prepared<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    prepared: usize,
) -> bool {
    let mut ok = true;
    for prev in plan.steps.iter().take(prepared).rev() {
        if client
            .abort_keyed(plan.tx_id.clone(), prev.key.clone())
            .await
            .is_err()
        {
            ok = false;
        }
    }
    ok
}

fn is_no_prepared(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Server(msg) if msg.contains("no prepared command for transaction key")
    )
}

async fn prepare_step<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    step: usize,
    journal: Option<&dyn TwoPhaseJournal>,
) -> Result<(), TwoPhaseError> {
    let item = &plan.steps[step];
    client
        .prepare_keyed(plan.tx_id.clone(), item.key.clone(), item.command.clone())
        .await
        .map_err(|source| TwoPhaseError::Prepare {
            step,
            prepared: step,
            source,
        })?;
    if let Some(j) = journal {
        j.on_prepared(&plan.tx_id, step, plan.steps.len()).await?;
    }
    Ok(())
}

async fn commit_step<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    step: usize,
    journal: Option<&dyn TwoPhaseJournal>,
) -> Result<Vec<u8>, TwoPhaseError> {
    let item = &plan.steps[step];
    let bytes = client
        .commit_keyed(plan.tx_id.clone(), item.key.clone())
        .await
        .map_err(|source| TwoPhaseError::Commit {
            step,
            committed: step,
            source,
        })?;
    if let Some(j) = journal {
        j.on_committed(&plan.tx_id, step, plan.steps.len()).await?;
    }
    Ok(bytes)
}

async fn prepare_or_commit_step<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    step: usize,
    journal: Option<&dyn TwoPhaseJournal>,
    probe: bool,
) -> Result<Vec<u8>, TwoPhaseError> {
    let item = &plan.steps[step];
    if probe {
        match client
            .commit_keyed(plan.tx_id.clone(), item.key.clone())
            .await
        {
            Ok(bytes) => {
                if let Some(j) = journal {
                    j.on_prepared(&plan.tx_id, step, plan.steps.len()).await?;
                    j.on_committed(&plan.tx_id, step, plan.steps.len()).await?;
                }
                return Ok(bytes);
            }
            Err(err) if is_no_prepared(&err) => {}
            Err(source) => {
                return Err(TwoPhaseError::Commit {
                    step,
                    committed: step,
                    source,
                });
            }
        }
    }
    prepare_step(client, plan, step, journal).await?;
    commit_step(client, plan, step, journal).await
}

/// Execute prepare-all then commit-all, aborting prepared steps on prepare failure.
///
/// # Errors
/// Returns [`TwoPhaseError::Plan`] when the plan is invalid,
/// [`TwoPhaseError::Prepare`] or [`TwoPhaseError::Commit`] when a shard RPC fails,
/// or [`TwoPhaseError::Journal`] when journal persistence fails.
pub async fn propose_cross_shard_2pc<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    group_for_key: impl Fn(&[u8]) -> Option<u32>,
) -> Result<Vec<Vec<u8>>, TwoPhaseError> {
    propose_cross_shard_2pc_with_opts(client, plan, group_for_key, RunTwoPhaseOpts::default()).await
}

/// Like [`propose_cross_shard_2pc`] with an optional client journal.
///
/// # Errors
/// Same as [`propose_cross_shard_2pc`].
pub async fn propose_cross_shard_2pc_with_opts<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    group_for_key: impl Fn(&[u8]) -> Option<u32>,
    opts: RunTwoPhaseOpts<'_>,
) -> Result<Vec<Vec<u8>>, TwoPhaseError> {
    validate_two_phase_plan(plan, group_for_key)?;
    let journal = opts.journal;

    for (step, item) in plan.steps.iter().enumerate() {
        if let Err(source) = client
            .prepare_keyed(plan.tx_id.clone(), item.key.clone(), item.command.clone())
            .await
        {
            if !abort_prepared(client, plan, step).await {
                emit_stuck(opts.on_event, step, step);
            }
            return Err(TwoPhaseError::Prepare {
                step,
                prepared: step,
                source,
            });
        }
        if let Some(j) = journal {
            j.on_prepared(&plan.tx_id, step, plan.steps.len()).await?;
        }
    }

    if let Some(on) = opts.on_event {
        on(TwoPhaseEvent::Prepared {
            steps: plan.steps.len(),
        });
    }

    let mut responses = Vec::with_capacity(plan.steps.len());
    for (step, _item) in plan.steps.iter().enumerate() {
        match commit_step(client, plan, step, journal).await {
            Ok(bytes) => responses.push(bytes),
            Err(err) => {
                emit_stuck(opts.on_event, plan.steps.len(), step);
                return Err(err);
            }
        }
    }
    if let Some(j) = journal {
        j.on_completed(&plan.tx_id).await?;
    }
    Ok(responses)
}

/// Continue a cross-shard 2PC after partial progress or coordinator restart.
///
/// With a [`TwoPhaseJournal`], skips consecutive prepared/committed prefixes recorded
/// client-side. With `probe = true` (default), steps without journal state attempt
/// `commit_keyed` first so a durable server-side prepare can be picked up after restart.
///
/// # Errors
/// Returns [`TwoPhaseError::Plan`] when the plan is invalid,
/// [`TwoPhaseError::Prepare`] or [`TwoPhaseError::Commit`] when a shard RPC fails,
/// or [`TwoPhaseError::Journal`] when journal persistence fails.
pub async fn resume_cross_shard_2pc<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    group_for_key: impl Fn(&[u8]) -> Option<u32>,
    opts: ResumeTwoPhaseOpts<'_>,
) -> Result<Vec<Vec<u8>>, TwoPhaseError> {
    validate_two_phase_plan(plan, group_for_key)?;
    let journal = opts.journal;

    let (prepared_through, committed_through) = if let Some(j) = journal {
        match j.load(&plan.tx_id).await? {
            Some(rec) => (rec.prepared_steps as usize, rec.committed_steps as usize),
            None => (0, 0),
        }
    } else {
        (0, 0)
    };

    let mut responses = Vec::with_capacity(plan.steps.len());

    for _step in 0..committed_through.min(plan.steps.len()) {
        responses.push(Vec::new());
    }

    for step in committed_through..prepared_through.min(plan.steps.len()) {
        responses.push(commit_step(client, plan, step, journal).await?);
    }

    for step in prepared_through..plan.steps.len() {
        match prepare_or_commit_step(client, plan, step, journal, opts.probe).await {
            Ok(bytes) => responses.push(bytes),
            Err(err) => {
                emit_stuck(opts.on_event, step, step);
                return Err(err);
            }
        }
    }

    if let Some(j) = journal {
        j.on_completed(&plan.tx_id).await?;
    }
    Ok(responses)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    use crafty_net::{Route, Transport, TransportError, decode_body, encode_body};
    use crafty_proto::{ClientRequest, ClientResponse, NodeId};

    use super::*;
    use crate::{RemoteClient, RetryPolicy};

    type PreparedKeys = HashSet<(Vec<u8>, Vec<u8>)>;

    struct TwoPhaseScript {
        prepared: Arc<Mutex<PreparedKeys>>,
    }

    impl Transport for TwoPhaseScript {
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
            let prepared = Arc::clone(&self.prepared);
            Box::pin(async move {
                match request {
                    ClientRequest::TwoPhasePrepare { tx_id, key, .. } => {
                        prepared.lock().expect("lock").insert((tx_id, key));
                        encode_body(&ClientResponse::Ok(Vec::new())).map_err(TransportError::Wire)
                    }
                    ClientRequest::TwoPhaseCommit { tx_id, key } => {
                        if prepared.lock().expect("lock").contains(&(tx_id, key)) {
                            encode_body(&ClientResponse::Ok(vec![1])).map_err(TransportError::Wire)
                        } else {
                            encode_body(&ClientResponse::Error(
                                "no prepared command for transaction key".into(),
                            ))
                            .map_err(TransportError::Wire)
                        }
                    }
                    other => Err(TransportError::Io(format!("unexpected request: {other:?}"))),
                }
            })
        }
    }

    impl TwoPhaseScript {
        fn new() -> Self {
            Self {
                prepared: Arc::new(Mutex::new(HashSet::new())),
            }
        }

        fn mark_prepared(&self, tx_id: &[u8], key: &[u8]) {
            self.prepared
                .lock()
                .expect("lock")
                .insert((tx_id.to_vec(), key.to_vec()));
        }
    }

    fn sample_plan() -> TwoPhasePlan {
        TwoPhasePlan {
            tx_id: b"tx-resume".to_vec(),
            steps: vec![
                crafty_core::TwoPhaseStep {
                    key: b"a".to_vec(),
                    command: vec![1],
                },
                crafty_core::TwoPhaseStep {
                    key: b"b".to_vec(),
                    command: vec![2],
                },
            ],
        }
    }

    #[tokio::test]
    async fn resume_probes_commit_before_prepare() {
        let script = Arc::new(TwoPhaseScript::new());
        script.mark_prepared(b"tx-resume", b"a");
        let client = RemoteClient::new(script, [NodeId(1)]).with_retry(RetryPolicy {
            max_attempts: 1,
            ..Default::default()
        });
        let plan = sample_plan();
        let out =
            resume_cross_shard_2pc(&client, &plan, |_| Some(0), ResumeTwoPhaseOpts::default())
                .await
                .expect("resume");
        assert_eq!(out.len(), 2);
    }

    #[tokio::test]
    async fn resume_skips_journal_committed_prefix() {
        let script = Arc::new(TwoPhaseScript::new());
        script.mark_prepared(b"tx-resume", b"b");
        let client = RemoteClient::new(script, [NodeId(1)]).with_retry(RetryPolicy {
            max_attempts: 1,
            ..Default::default()
        });
        let journal = InMemoryTwoPhaseJournal::default();
        journal
            .on_prepared(b"tx-resume", 0, 2)
            .await
            .expect("prep 0");
        journal
            .on_prepared(b"tx-resume", 1, 2)
            .await
            .expect("prep 1");
        journal
            .on_committed(b"tx-resume", 0, 2)
            .await
            .expect("commit 0");

        let plan = sample_plan();
        let out = resume_cross_shard_2pc(
            &client,
            &plan,
            |_| Some(0),
            ResumeTwoPhaseOpts {
                journal: Some(&journal),
                probe: false,
                on_event: None,
            },
        )
        .await
        .expect("resume");
        assert_eq!(out.len(), 2);
        assert_eq!(out[1], vec![1]);
    }
}
