//! Route table for recurring schedule admin.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use http::StatusCode;
use trembita_jobs::RecurringJob;
use trembita_proto::RecurringScheduleWire;

use crate::routing::{HttpError, RequestCtx, Response, RouteTable};
use crate::types::{ScheduleJson, ScheduleListResponse, SchedulesApiError};

/// Shared state for schedule routes.
pub struct SchedulesApiState {
    pub(crate) list: crate::ListSchedulesFn,
    pub(crate) upsert: crate::UpsertScheduleFn,
    pub(crate) remove: crate::RemoveScheduleFn,
}

/// Route table for schedule admin routes.
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn route_table(state: Arc<SchedulesApiState>) -> RouteTable {
    let s1 = Arc::clone(&state);
    let s2 = Arc::clone(&state);
    let s3 = Arc::clone(&state);
    RouteTable::new()
        .get("/jobs/{stream}/schedules", move |ctx| {
            let state = Arc::clone(&s1);
            async move { list_schedules(state, ctx).await }
        })
        .put("/jobs/{stream}/schedules/{name}", move |ctx| {
            let state = Arc::clone(&s2);
            async move { put_schedule(state, ctx).await }
        })
        .delete("/jobs/{stream}/schedules/{name}", move |ctx| {
            let state = Arc::clone(&s3);
            async move { delete_schedule(state, ctx).await }
        })
}

async fn list_schedules(
    state: Arc<SchedulesApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match list_schedules_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn list_schedules_inner(
    state: &SchedulesApiState,
    ctx: RequestCtx,
) -> Result<Response, SchedulesApiError> {
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| SchedulesApiError::BadRequest("missing stream".into()))?
        .to_string();
    let wires = (state.list)(stream)
        .await
        .map_err(|e| SchedulesApiError::Queue(e.to_string()))?;
    let schedules = wires.into_iter().map(wire_to_json).collect();
    let json = serde_json::to_value(ScheduleListResponse { schedules })
        .map_err(|e| SchedulesApiError::BadRequest(e.to_string()))?;
    Ok(Response::json(StatusCode::OK, json))
}

async fn put_schedule(
    state: Arc<SchedulesApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match put_schedule_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn put_schedule_inner(
    state: &SchedulesApiState,
    ctx: RequestCtx,
) -> Result<Response, SchedulesApiError> {
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| SchedulesApiError::BadRequest("missing stream".into()))?
        .to_string();
    let name = ctx
        .params()
        .get("name")
        .ok_or_else(|| SchedulesApiError::BadRequest("missing schedule name".into()))?
        .to_string();
    let mut body: ScheduleJson = serde_json::from_slice(ctx.body().as_ref())
        .map_err(|e| SchedulesApiError::BadRequest(format!("invalid json: {e}")))?;
    body.name = name;
    let job = json_to_recurring_job(&body)?;
    (state.upsert)(stream, job)
        .await
        .map_err(|e| SchedulesApiError::Queue(e.to_string()))?;
    Ok(Response::text(StatusCode::OK, "ok"))
}

async fn delete_schedule(
    state: Arc<SchedulesApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match delete_schedule_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn delete_schedule_inner(
    state: &SchedulesApiState,
    ctx: RequestCtx,
) -> Result<Response, SchedulesApiError> {
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| SchedulesApiError::BadRequest("missing stream".into()))?
        .to_string();
    let name = ctx
        .params()
        .get("name")
        .ok_or_else(|| SchedulesApiError::BadRequest("missing schedule name".into()))?
        .to_string();
    (state.remove)(stream, name)
        .await
        .map_err(|e| SchedulesApiError::Queue(e.to_string()))?;
    Ok(Response::text(StatusCode::OK, "ok"))
}

fn wire_to_json(wire: RecurringScheduleWire) -> ScheduleJson {
    let payload = String::from_utf8(wire.payload.clone()).ok();
    let payload_b64 = payload.is_none().then(|| BASE64.encode(&wire.payload));
    ScheduleJson {
        name: wire.name,
        cron: wire.cron,
        every_days: wire.every_days,
        anchor_ms: wire.anchor_ms,
        payload,
        payload_b64,
        priority: wire.priority.0,
        max_attempts: wire.max_attempts.0,
        enabled: wire.enabled,
        next_run_ms: wire.next_run_ms,
    }
}

fn json_to_recurring_job(body: &ScheduleJson) -> Result<RecurringJob, SchedulesApiError> {
    let payload = decode_payload(body)?;
    Ok(RecurringJob {
        name: body.name.clone(),
        cron: body.cron.clone(),
        every_days: body.every_days,
        anchor_ms: body.anchor_ms,
        payload,
        priority: body.priority,
        max_attempts: body.max_attempts,
        enabled: body.enabled,
    })
}

fn decode_payload(body: &ScheduleJson) -> Result<Vec<u8>, SchedulesApiError> {
    match (&body.payload, &body.payload_b64) {
        (Some(text), None) => Ok(text.as_bytes().to_vec()),
        (None, Some(b64)) => BASE64
            .decode(b64)
            .map_err(|e| SchedulesApiError::BadRequest(format!("invalid payload_b64: {e}"))),
        (Some(_), Some(_)) => Err(SchedulesApiError::BadRequest(
            "provide only one of payload or payload_b64".into(),
        )),
        (None, None) => Ok(Vec::new()),
    }
}
