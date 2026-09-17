//! Queued capability with cap-store idempotency (R4) — reference for durable side effects.

use serde::{Deserialize, Serialize};
use trembita::capstore::{store_get, store_set};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest, OpCtx};

#[derive(Default)]
struct TaskState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TaskDone;

#[derive(Debug, Serialize, Deserialize)]
pub struct RunTask {
    /// Idempotency key (also used for keyed routing).
    pub task_id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskAck {
    pub ok: bool,
}

#[cap_handler(group = "tasks", key = "task_id")]
async fn run_task(msg: RunTask, ctx: OpCtx<'_>, _state: &mut TaskState) -> Result<TaskAck, CapError> {
    let store = ctx.require_store()?;
    let marker = format!("task:{}", msg.task_id);
    if store_get::<TaskDone>(&*store, &marker)
        .await
        .map_err(CapError::handler)?
        .is_some()
    {
        return Ok(TaskAck { ok: true });
    }

    // Domain work here (set marker before ack for at-least-once safety).
    store_set(&*store, &marker, &TaskDone, None)
        .await
        .map_err(CapError::handler)?;
    Ok(TaskAck { ok: true })
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<TaskState>::for_cap::<RunTask>().default_queue_for::<RunTask>(),
        run_task_register,
    ))
}
