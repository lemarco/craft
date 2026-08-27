//! Cross-shard two-phase commit coordinator (optional Tier 2 increment).

use std::future::Future;

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

/// Execute prepare-all then commit-all, aborting prepared steps on prepare failure.
pub async fn propose_cross_shard_2pc<C: TwoPhaseClient>(
    client: &C,
    plan: &TwoPhasePlan,
    group_for_key: impl Fn(&[u8]) -> Option<u32>,
) -> Result<Vec<Vec<u8>>, TwoPhaseError> {
    validate_two_phase_plan(plan, group_for_key)?;

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
    }

    let mut responses = Vec::with_capacity(plan.steps.len());
    for (step, item) in plan.steps.iter().enumerate() {
        match client
            .commit_keyed(plan.tx_id.clone(), item.key.clone())
            .await
        {
            Ok(bytes) => responses.push(bytes),
            Err(source) => {
                return Err(TwoPhaseError::Commit {
                    step,
                    committed: step,
                    source,
                });
            }
        }
    }
    Ok(responses)
}
