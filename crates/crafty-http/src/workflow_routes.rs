//! Axum routes for keyed saga run / resume.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};

use crate::WorkflowsApiState;
use crate::workflow_types::{SagaBody, WorkflowAccepted, WorkflowsApiError};

/// Axum sub-router for workflow trigger routes.
pub fn workflows_router() -> Router<Arc<WorkflowsApiState>> {
    Router::new()
        .route("/health", get(get_health))
        .route("/workflows/run", post(post_run))
        .route("/workflows/resume", post(post_resume))
}

async fn get_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn post_run(
    State(state): State<Arc<WorkflowsApiState>>,
    Json(body): Json<SagaBody>,
) -> Result<Json<WorkflowAccepted>, WorkflowsApiError> {
    let result = (state.run)(body.saga_id.clone()).await?;
    Ok(Json(result))
}

async fn post_resume(
    State(state): State<Arc<WorkflowsApiState>>,
    Json(body): Json<SagaBody>,
) -> Result<Json<WorkflowAccepted>, WorkflowsApiError> {
    let result = (state.resume)(body.saga_id.clone()).await?;
    Ok(Json(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::future;
    use tower::ServiceExt;

    fn test_state(
        run: crate::RunWorkflowFn,
        resume: crate::ResumeWorkflowFn,
    ) -> Arc<WorkflowsApiState> {
        Arc::new(WorkflowsApiState { run, resume })
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
        let app = workflows_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/workflows/run")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"saga_id":"onboard-1"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_health_returns_ok() {
        let state = test_state(
            Arc::new(|_| Box::pin(future::ready(Err(WorkflowsApiError::Failed("x".into()))))),
            Arc::new(|_| Box::pin(future::ready(Err(WorkflowsApiError::Failed("x".into()))))),
        );
        let app = workflows_router().with_state(state);
        let req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::from(""))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
