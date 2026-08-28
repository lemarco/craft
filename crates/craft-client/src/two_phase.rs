//! Cross-shard two-phase commit coordinator (optional Tier 2 increment).

use std::future::Future;
use std::pin::Pin;

use craft_core::{TwoPhasePlan, TwoPhasePlanError, validate_two_phase_plan};

use crate::{ClientError, KeyedClient};

/// Extension of [`KeyedClient`] for limited cross-shard 2PC.
pub trait TwoPhaseClient: KeyedClient {
    fn prepare_keyed(
        &self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
        command: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;

    fn commit_keyed(
        &self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;

    fn abort_keyed(
        &self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;
}

/// Why a cross-shard 2PC attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum TwoPhaseError {
    #[error("invalid 2PC plan: {0}")]
    Plan(#[from] TwoPhasePlanError),
    #[error("2PC journal error: {0}")]
    Journal(#[from] TwoPhaseJournalError),
    #[error("2PC prepare failed at step {step} after {prepared} prepare(s): {source}")]
    Prepare {
        step: usize,
        prepared: usize,
        #[source]
        source: ClientError,
    },
    #[error("2PC commit failed at step {step} after {committed} commit(s): {source}")]
    Commit {
        step: usize,
        committed: usize,
        #[source]
        source: ClientError,
    },
}

/// Journal persistence failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TwoPhaseJournalError {
    #[error("journal codec error: {0}")]
    Codec(String),
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
    fn on_prepared<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>>;

    fn on_committed<'a>(
        &'a self,
        tx_id: &'a [u8],
        step: usize,
        total: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>>;

    fn on_completed<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), TwoPhaseJournalError>> + Send + 'a>>;

    fn load<'a>(
        &'a self,
        tx_id: &'a [u8],
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError>> + Send + 'a>,
    >;
}

/// In-memory 2PC journal (tests and single-process coordinators).
#[derive(Debug, Default)]
pub struct InMemoryTwoPhaseJournal {
    records: std::sync::Mutex<Vec<TwoPhaseJournalRecord>>,
}

impl InMemoryTwoPhaseJournal {
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
                rec.prepared_steps = (step as u32 + 1).max(rec.prepared_steps);
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
                rec.committed_steps = (step as u32 + 1).max(rec.committed_steps);
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
        Box<dyn Future<Output = Result<Option<TwoPhaseJournalRecord>, TwoPhaseJournalError>> + Send + 'a>,
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
    pub journal: Option<&'a dyn TwoPhaseJournal>,
}

/// Hooks for [`resume_cross_shard_2pc`].
#[derive(Clone, Copy)]
pub struct ResumeTwoPhaseOpts<'a> {
    pub journal: Option<&'a dyn TwoPhaseJournal>,
    /// When `true`, try `commit_keyed` before `prepare_keyed` for unknown steps.
    pub probe: bool,
}

impl<'a> Default for ResumeTwoPhaseOpts<'a> {
    fn default() -> Self {
        Self {
            journal: None,
            probe: true,
        }
    }
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
        .prepare_keyed(
            plan.tx_id.clone(),
            item.key.clone(),
            item.command.clone(),
        )
        .await
        .map_err(|source| TwoPhaseError::Prepare {
            step,
            prepared: step,
            source,
        })?;
    if let Some(j) = journal {
        j.on_prepared(&plan.tx_id, step, plan.steps.len())
            .await?;
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
        j.on_committed(&plan.tx_id, step, plan.steps.len())
            .await?;
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
                    j.on_prepared(&plan.tx_id, step, plan.steps.len())
                        .await?;
                    j.on_committed(&plan.tx_id, step, plan.steps.len())
                        .await?;
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
pub async fn propose_cross_shard_2pc<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    group_for_key: impl Fn(&[u8]) -> Option<u32>,
) -> Result<Vec<Vec<u8>>, TwoPhaseError> {
    propose_cross_shard_2pc_with_opts(
        client,
        plan,
        group_for_key,
        RunTwoPhaseOpts::default(),
    )
    .await
}

/// Like [`propose_cross_shard_2pc`] with an optional client journal.
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
            for prev in plan.steps.iter().take(step).rev() {
                let _ = client
                    .abort_keyed(plan.tx_id.clone(), prev.key.clone())
                    .await;
            }
            return Err(TwoPhaseError::Prepare {
                step,
                prepared: step,
                source,
            });
        }
        if let Some(j) = journal {
            j.on_prepared(&plan.tx_id, step, plan.steps.len())
                .await?;
        }
    }

    let mut responses = Vec::with_capacity(plan.steps.len());
    for (step, _item) in plan.steps.iter().enumerate() {
        responses.push(commit_step(client, plan, step, journal).await?);
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
        responses.push(
            prepare_or_commit_step(client, plan, step, journal, opts.probe).await?,
        );
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

    use craft_net::{Route, Transport, TransportError, decode_body, encode_body};
    use craft_proto::{ClientRequest, ClientResponse, NodeId};

    use super::*;
    use crate::{RemoteClient, RetryPolicy};

    struct TwoPhaseScript {
        prepared: Arc<Mutex<HashSet<(Vec<u8>, Vec<u8>)>>>,
    }

    impl Transport for TwoPhaseScript {
        fn send(
            &self,
            _peer: NodeId,
            _route: Route,
            body: craft_net::transport::Body,
        ) -> craft_net::transport::BoxFuture<
            'static,
            Result<craft_net::transport::Body, TransportError>,
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
                craft_core::TwoPhaseStep {
                    key: b"a".to_vec(),
                    command: vec![1],
                },
                craft_core::TwoPhaseStep {
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
        let out = resume_cross_shard_2pc(&client, &plan, |_| Some(0), ResumeTwoPhaseOpts::default())
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
            },
        )
        .await
        .expect("resume");
        assert_eq!(out.len(), 2);
        assert_eq!(out[1], vec![1]);
    }
}
