//! Dispatch standard [`WorkTrigger`] payloads from queue consumers.

use std::sync::Arc;

use trembita_client::SagaOutcome;
use trembita_jobs::{WorkTrigger, WorkTriggerError};
use trembita_proto::JobId;

use trembita_jobs::QueueError;

use crate::TrembitaApp;

/// Result of handling one job payload as a built-in trigger.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// Bytes were not a [`WorkTrigger`]; handle as domain payload.
    NotTrigger,
    /// Workflow completed (or failed in a saga-visible way).
    Workflow(SagaOutcome),
    /// Follow-up job was enqueued.
    Enqueued(JobId),
}

/// Errors from [`dispatch_work_trigger`].
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// Trigger JSON was recognized but invalid.
    #[error(transparent)]
    Trigger(#[from] WorkTriggerError),
    /// Queue enqueue failed.
    #[error("enqueue: {0}")]
    Enqueue(String),
    /// Workflow execution failed.
    #[error("workflow: {0}")]
    Workflow(String),
}

/// If `payload` is a [`WorkTrigger`], run the workflow or enqueue follow-up work.
///
/// Returns [`DispatchOutcome::NotTrigger`] when the body is not JSON trigger wire — decode your
/// app `enum` in that branch.
///
/// # Errors
/// Returns [`DispatchError`] on malformed trigger versions, enqueue, or saga failures.
pub async fn dispatch_work_trigger(
    app: &Arc<TrembitaApp>,
    payload: &[u8],
) -> Result<DispatchOutcome, DispatchError> {
    let Some(trigger) = WorkTrigger::decode(payload)? else {
        return Ok(DispatchOutcome::NotTrigger);
    };
    match trigger {
        WorkTrigger::Workflow { saga_id } => {
            let outcome = app
                .run_workflow_id(&saga_id)
                .await
                .map_err(|e| DispatchError::Workflow(e.to_string()))?;
            Ok(DispatchOutcome::Workflow(outcome))
        }
        WorkTrigger::Enqueue { stream, payload } => {
            let job_id = app
                .enqueue(&stream, &payload)
                .await
                .map_err(|e: QueueError| DispatchError::Enqueue(e.to_string()))?;
            Ok(DispatchOutcome::Enqueued(job_id))
        }
    }
}
