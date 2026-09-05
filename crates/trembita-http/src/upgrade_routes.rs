//! Route table for rolling self-update.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::StatusCode;
use trembita_core::ArtifactManifest;

use crate::AuthFn;
use crate::routing::{AuthMode, HttpError, RequestCtx, Response, RouteTable};
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

    /// Route table (`GET/POST /cluster/upgrade…`).
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        let table = route_table(Arc::new(UpgradeApiState {
            view: Arc::clone(&self.view),
            set_desired: Arc::clone(&self.set_desired),
            auth: None,
        }));
        if self.auth.is_some() {
            table.with_auth_mode(AuthMode::Identity)
        } else {
            table
        }
    }

    /// State handle for [`Self::route_table`].
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

    fn clone_state(&self) -> UpgradeApiState {
        UpgradeApiState {
            view: Arc::clone(&self.view),
            set_desired: Arc::clone(&self.set_desired),
            auth: self.auth.clone(),
        }
    }
}

/// Route table for upgrade routes.
#[must_use]
pub fn route_table(state: Arc<UpgradeApiState>) -> RouteTable {
    let get_state = Arc::clone(&state);
    let post_state = state;
    RouteTable::new()
        .get("/cluster/upgrade", move |ctx| {
            let state = Arc::clone(&get_state);
            async move { get_upgrade(state, ctx).await }
        })
        .post("/cluster/upgrade/desired", move |ctx| {
            let state = Arc::clone(&post_state);
            async move { post_desired(state, ctx).await }
        })
}

async fn get_upgrade(state: Arc<UpgradeApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_upgrade_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_upgrade_inner(
    state: &UpgradeApiState,
    ctx: RequestCtx,
) -> Result<Response, UpgradeApiError> {
    let view = (state.view)().await?;
    let json = serde_json::to_value(view)
        .map_err(|e| UpgradeApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::OK, json))
}

async fn post_desired(state: Arc<UpgradeApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_desired_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_desired_inner(
    state: &UpgradeApiState,
    ctx: RequestCtx,
) -> Result<Response, UpgradeApiError> {
    let parsed: SetDesiredBody = ctx
        .json()
        .map_err(|e| UpgradeApiError::BadRequest(e.message().to_string()))?;
    (state.set_desired)(parsed.into()).await?;
    Ok(Response::status(StatusCode::ACCEPTED))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use http::Method;
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
        let table = route_table(Arc::new(UpgradeApiState {
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
        }));
        let resp = table
            .dispatch_open(
                &Method::GET,
                "/cluster/upgrade",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_upgrade_rejects_without_auth_when_configured() {
        use crate::auth_fn_to_identity;
        use crate::routing::DispatchGates;

        let auth: AuthFn =
            Arc::new(|_, _, _| Box::pin(async { Err(JobsApiError::Unauthorized("nope".into())) }));
        let identity = auth_fn_to_identity(Arc::clone(&auth));
        let table = route_table(state_with_auth(auth)).with_auth_mode(AuthMode::Identity);
        let gates = DispatchGates {
            session_gate: None,
            identity: Some(&identity),
        };
        let err = table
            .dispatch(
                &Method::GET,
                "/cluster/upgrade",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
                &gates,
            )
            .await
            .expect_err("401");
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn post_desired_accepts_manifest() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&called);
        let table = route_table(Arc::new(UpgradeApiState {
            view: Arc::new(|| Box::pin(async { Err(UpgradeApiError::Backend("unused".into())) })),
            set_desired: Arc::new(move |_| {
                let flag = Arc::clone(&flag);
                Box::pin(async move {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            }),
            auth: None,
        }));
        let body = r#"{"app_version":"1.0.0","url":"file:///x","sha256_hex":"00"}"#;
        let resp = table
            .dispatch_open(
                &Method::POST,
                "/cluster/upgrade/desired",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::from(body),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::ACCEPTED);
        assert!(called.load(Ordering::SeqCst));
    }
}
