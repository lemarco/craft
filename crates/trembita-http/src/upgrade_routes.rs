//! Axum routes for rolling self-update.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use trembita_core::ArtifactManifest;

use crate::AuthFn;
use crate::upgrade_types::{SetDesiredBody, UpgradeApiError, UpgradeStatusResponse};

/// Async view hook for [`UpgradeApi`].
pub type UpgradeViewFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<UpgradeStatusResponse, UpgradeApiError>> + Send>>
        + Send
        + Sync,
>;

/// Async set-desired hook for [`UpgradeApi`].
pub type SetDesiredFn = Arc<
    dyn Fn(ArtifactManifest) -> Pin<Box<dyn Future<Output = Result<(), UpgradeApiError>> + Send>>
        + Send
        + Sync,
>;

/// Shared handler state.
pub struct UpgradeApiState {
    /// `GET /cluster/upgrade`
    pub view: UpgradeViewFn,
    /// `POST /cluster/upgrade/desired`
    pub set_desired: SetDesiredFn,
    /// Optional auth hook (required for production fleet mutation).
    pub auth: Option<AuthFn>,
}

/// Cluster upgrade HTTP API.
pub struct UpgradeApi {
    view: UpgradeViewFn,
    set_desired: SetDesiredFn,
    auth: Option<AuthFn>,
}

impl UpgradeApi {
    /// Wire view + set-desired hooks from your Raft client / coordinator.
    #[must_use]
    pub fn new(view: UpgradeViewFn, set_desired: SetDesiredFn) -> Self {
        Self {
            view,
            set_desired,
            auth: None,
        }
    }

    /// Require [`AuthFn`] on upgrade routes (recommended for any exposed listener).
    #[must_use]
    pub fn with_auth(mut self, auth: AuthFn) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Axum sub-router (`GET/POST /cluster/upgrade…`).
    pub fn router(&self) -> Router<Arc<UpgradeApiState>> {
        upgrade_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> UpgradeApiState {
        UpgradeApiState {
            view: self.view,
            set_desired: self.set_desired,
            auth: self.auth,
        }
    }

    /// Like [`Self::into_state`] with an explicit auth hook.
    #[must_use]
    pub fn into_state_with_auth(self, auth: Option<AuthFn>) -> UpgradeApiState {
        UpgradeApiState {
            view: self.view,
            set_desired: self.set_desired,
            auth,
        }
    }
}

/// Axum sub-router for upgrade routes.
pub fn upgrade_router() -> Router<Arc<UpgradeApiState>> {
    Router::new()
        .route("/cluster/upgrade", get(get_upgrade))
        .route("/cluster/upgrade/desired", post(post_desired))
}

async fn authorize(
    state: &UpgradeApiState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<(), UpgradeApiError> {
    if let Some(auth) = &state.auth {
        auth(method.clone(), uri.clone(), headers.clone())
            .await
            .map_err(|e| UpgradeApiError::Unauthorized(e.to_string()))?;
    }
    Ok(())
}

async fn get_upgrade(
    State(state): State<Arc<UpgradeApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<UpgradeStatusResponse>, UpgradeApiError> {
    authorize(state.as_ref(), &method, &uri, &headers).await?;
    let view = (state.view)().await?;
    Ok(Json(view))
}

async fn post_desired(
    State(state): State<Arc<UpgradeApiState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Json(parsed): Json<SetDesiredBody>,
) -> Result<impl IntoResponse, UpgradeApiError> {
    authorize(state.as_ref(), &method, &uri, &headers).await?;
    (state.set_desired)(parsed.into()).await?;
    Ok(StatusCode::ACCEPTED)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use trembita_core::UpgradeView;

    use std::collections::BTreeSet;

    use super::*;
    use crate::JobsApiError;

    fn state_with_auth(auth: AuthFn) -> Arc<UpgradeApiState> {
        Arc::new(UpgradeApiState {
            view: Arc::new(|| {
                Box::pin(async {
                    Ok(UpgradeView {
                        desired: None,
                        granted: None,
                        completed: BTreeSet::default(),
                        pending: vec![],
                        fleet_ready: true,
                        aborted: None,
                    })
                })
            }),
            set_desired: Arc::new(|_| Box::pin(async { Ok(()) })),
            auth: Some(auth),
        })
    }

    #[tokio::test]
    async fn get_upgrade_returns_view_json() {
        let state = Arc::new(UpgradeApiState {
            view: Arc::new(|| {
                Box::pin(async {
                    Ok(UpgradeView {
                        desired: None,
                        granted: None,
                        completed: BTreeSet::default(),
                        pending: vec![],
                        fleet_ready: true,
                        aborted: None,
                    })
                })
            }),
            set_desired: Arc::new(|_| Box::pin(async { Ok(()) })),
            auth: None,
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
    async fn get_upgrade_rejects_without_auth_when_configured() {
        let auth: AuthFn =
            Arc::new(|_, _, _| Box::pin(async { Err(JobsApiError::Unauthorized("nope".into())) }));
        let app = upgrade_router().with_state(state_with_auth(auth));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/cluster/upgrade")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn post_desired_accepts_manifest() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&called);
        let state = Arc::new(UpgradeApiState {
            view: Arc::new(|| Box::pin(async { Err(UpgradeApiError::Backend("unused".into())) })),
            set_desired: Arc::new(move |_| {
                let flag = Arc::clone(&flag);
                Box::pin(async move {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            }),
            auth: None,
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
