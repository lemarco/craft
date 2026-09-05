//! Route table for keyed saga run / resume.

use std::sync::Arc;

use http::{Method, StatusCode, Uri};

use crate::WorkflowsApiState;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};
use crate::types::JobsApiError;
use crate::workflow_types::{SagaBody, WorkflowAccepted, WorkflowsApiError};

fn ctx_uri(ctx: &RequestCtx) -> Uri {
    ctx.path().parse().unwrap_or_else(|_| Uri::from_static("/"))
}

async fn authorize(state: &WorkflowsApiState, ctx: &RequestCtx) -> Result<(), WorkflowsApiError> {
    if let Some(auth) = &state.auth {
        auth(ctx.method().clone(), ctx_uri(ctx), ctx.headers().clone())
            .await
            .map_err(|e| match e {
                JobsApiError::Unauthorized(m) => WorkflowsApiError::Unauthorized(m),
                other => WorkflowsApiError::BadRequest(other.to_string()),
            })?;
    }
    Ok(())
}

/// Route table for workflow trigger routes.
#[must_use]
pub fn route_table(state: Arc<WorkflowsApiState>) -> RouteTable {
    let health_state = Arc::clone(&state);
    let run_state = Arc::clone(&state);
    let resume_state = state;
    RouteTable::new()
        .get("/health", move |ctx| {
            let state = Arc::clone(&health_state);
            async move { get_health(state, ctx).await }
        })
        .post("/workflows/run", move |ctx| {
            let state = Arc::clone(&run_state);
            async move { post_run(state, ctx).await }
        })
        .post("/workflows/resume", move |ctx| {
            let state = Arc::clone(&resume_state);
            async move { post_resume(state, ctx).await }
        })
}

async fn get_health(state: Arc<WorkflowsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_health_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_health_inner(
    state: &WorkflowsApiState,
    ctx: RequestCtx,
) -> Result<Response, WorkflowsApiError> {
    authorize(state, &ctx).await?;
    Ok(Response::text(StatusCode::OK, "ok"))
}

async fn post_run(state: Arc<WorkflowsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_run_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_run_inner(
    state: &WorkflowsApiState,
    ctx: RequestCtx,
) -> Result<Response, WorkflowsApiError> {
    authorize(state, &ctx).await?;
    let body: SagaBody = ctx
        .json()
        .map_err(|e| WorkflowsApiError::BadRequest(e.message().to_string()))?;
    let result = (state.run)(body.saga_id.clone()).await?;
    let json = serde_json::to_value(result)
        .map_err(|e| WorkflowsApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::OK, json))
}

async fn post_resume(
    state: Arc<WorkflowsApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match post_resume_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_resume_inner(
    state: &WorkflowsApiState,
    ctx: RequestCtx,
) -> Result<Response, WorkflowsApiError> {
    authorize(state, &ctx).await?;
    let body: SagaBody = ctx
        .json()
        .map_err(|e| WorkflowsApiError::BadRequest(e.message().to_string()))?;
    let result = (state.resume)(body.saga_id.clone()).await?;
    let json = serde_json::to_value(result)
        .map_err(|e| WorkflowsApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::OK, json))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Method;
    use std::collections::HashMap;
    use std::future;

    fn test_state(
        run: crate::RunWorkflowFn,
        resume: crate::ResumeWorkflowFn,
    ) -> Arc<WorkflowsApiState> {
        Arc::new(WorkflowsApiState {
            run,
            resume,
            auth: None,
        })
    }

    #[tokio::test]
    async fn post_run_returns_accepted() {
        let state = test_state(
            Arc::new(|id| {
                Box::pin(future::ready(Ok(WorkflowAccepted {
                    saga_id: id,
                    outcome: "completed".into(),
                })))
            }),
            Arc::new(|id| {
                Box::pin(future::ready(Ok(WorkflowAccepted {
                    saga_id: id,
                    outcome: "completed".into(),
                })))
            }),
        );
        let table = route_table(state);
        let resp = table
            .dispatch(
                &Method::POST,
                "/workflows/run",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::from(r#"{"saga_id":"onboard-1"}"#),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
    }
}
