//! Gateway middleware — acquire a compute token for each request.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use trembita_runtime::ComputeTokenPool;

/// Axum middleware: hold a compute token for the duration of the request.
pub async fn acquire_compute_token(
    State(pool): State<Arc<ComputeTokenPool>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let _guard = pool.acquire().await;
    next.run(request).await
}
