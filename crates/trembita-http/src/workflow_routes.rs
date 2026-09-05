//! Route table for keyed saga run / resume.

use std::sync::Arc;

use http::StatusCode;

use crate::WorkflowsApiState;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};
use crate::workflow_types::{SagaBody, WorkflowsApiError};

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
    _state: &WorkflowsApiState,
    _ctx: RequestCtx,
) -> Result<Response, WorkflowsApiError> {
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

    use crate::WorkflowAccepted;

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
            .dispatch_open(
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
