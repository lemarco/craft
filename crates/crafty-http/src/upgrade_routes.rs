//! Axum routes for rolling self-update.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Router, http::StatusCode};
use crafty_core::ArtifactManifest;

use crate::upgrade_types::{SetDesiredBody, UpgradeApiError, UpgradeStatusResponse};

/// Async view hook for [`UpgradeApi`].
pub type UpgradeViewFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<UpgradeStatusResponse, UpgradeApiError>> + Send>>
        + Send
        + Sync,
>;

/// Async set-desired hook for [`UpgradeApi`].
pub type SetDesiredFn = Arc<
    dyn Fn(
            ArtifactManifest,
        ) -> Pin<Box<dyn Future<Output = Result<(), UpgradeApiError>> + Send>>
        + Send
        + Sync,
>;

/// Shared handler state.
pub struct UpgradeApiState {
    /// `GET /cluster/upgrade`
    pub view: UpgradeViewFn,
    /// `POST /cluster/upgrade/desired`
    pub set_desired: SetDesiredFn,
}

/// Cluster upgrade HTTP API.
pub struct UpgradeApi {
    view: UpgradeViewFn,
    set_desired: SetDesiredFn,
}

impl UpgradeApi {
    /// Wire view + set-desired hooks from your Raft client / coordinator.
    #[must_use]
    pub fn new(view: UpgradeViewFn, set_desired: SetDesiredFn) -> Self {
        Self { view, set_desired }
    }

    /// Axum sub-router (`GET/POST /cluster/upgrade…`).
    #[must_use]
    pub fn router(&self) -> Router<Arc<UpgradeApiState>> {
        upgrade_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> UpgradeApiState {
        UpgradeApiState {
            view: self.view,
            set_desired: self.set_desired,
        }
    }
}

/// Axum sub-router for upgrade routes.
pub fn upgrade_router() -> Router<Arc<UpgradeApiState>> {
    Router::new()
        .route("/cluster/upgrade", get(get_upgrade))
        .route("/cluster/upgrade/desired", post(post_desired))
}

async fn get_upgrade(
    State(state): State<Arc<UpgradeApiState>>,
) -> Result<Json<UpgradeStatusResponse>, UpgradeApiError> {
    let view = (state.view)().await?;
    Ok(Json(view))
}

async fn post_desired(
    State(state): State<Arc<UpgradeApiState>>,
    Json(parsed): Json<SetDesiredBody>,
) -> Result<impl IntoResponse, UpgradeApiError> {
    (state.set_desired)(parsed.into()).await?;
    Ok(StatusCode::ACCEPTED)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use crafty_core::UpgradeView;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn get_upgrade_returns_view_json() {
        let state = Arc::new(UpgradeApiState {
            view: Arc::new(|| {
                Box::pin(async {
                    Ok(UpgradeView {
                        desired: None,
                        granted: None,
                        completed: Default::default(),
                        pending: vec![],
                        fleet_ready: true,
                        aborted: None,
                    })
                })
            }),
            set_desired: Arc::new(|_| Box::pin(async { Ok(()) })),
        });
        let app = upgrade_router().with_state(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/cluster/upgrade")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn post_desired_accepts_manifest() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&called);
        let state = Arc::new(UpgradeApiState {
            view: Arc::new(|| {
                Box::pin(async {
                    Err(UpgradeApiError::Backend("unused".into()))
                })
            }),
            set_desired: Arc::new(move |_| {
                let flag = Arc::clone(&flag);
                Box::pin(async move {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            }),
        });
        let app = upgrade_router().with_state(state);
        let body = r#"{"app_version":"1.0.0","url":"file:///x","sha256_hex":"00"}"#;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/cluster/upgrade/desired")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(called.load(Ordering::SeqCst));
    }
}
