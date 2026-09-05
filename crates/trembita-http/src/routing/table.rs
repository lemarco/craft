//! Route table — declarative method + path → handler mapping.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::Method;

use super::auth::{AuthMode, DispatchGates};
use super::ctx::{RequestCtx, Response};
use super::error::HttpError;
use super::handler::{ArcHandler, Handler};
use super::path::{PathParams, PathPattern};

/// One route entry in a [`RouteTable`].
#[derive(Clone)]
pub struct RouteEntry {
    method: Method,
    pattern: PathPattern,
    auth: AuthMode,
    handler: ArcHandler,
}

impl RouteEntry {
    /// Register one route.
    #[must_use]
    pub fn new(
        method: Method,
        path: &str,
        auth: AuthMode,
        handler: impl Handler + 'static,
    ) -> Self {
        Self {
            method,
            pattern: PathPattern::new(path),
            auth,
            handler: ArcHandler::new(handler),
        }
    }

    /// HTTP method.
    #[must_use]
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Path pattern.
    #[must_use]
    pub fn pattern(&self) -> &PathPattern {
        &self.pattern
    }

    /// Auth mode for this route.
    #[must_use]
    pub fn auth(&self) -> AuthMode {
        self.auth
    }

    /// Same route with a different auth mode (for [`RouteTable::merge_authed`]).
    #[must_use]
    pub fn with_auth(mut self, auth: AuthMode) -> Self {
        self.auth = auth;
        self
    }

    /// Snapshot for parity / diff tooling.
    #[must_use]
    pub fn descriptor(&self) -> super::diff::RouteDescriptor {
        super::diff::RouteDescriptor {
            method: self.method.clone(),
            path: self.pattern.template().to_string(),
            auth: self.auth,
        }
    }
}

/// Declarative route collection — the primary product-app HTTP construct in 0.4.0.
#[derive(Clone, Default)]
pub struct RouteTable {
    routes: Vec<RouteEntry>,
    fallback: Option<ArcHandler>,
    websocket: Option<(PathPattern, UpgradeFn)>,
}

type UpgradeFn = Arc<
    dyn Fn(
            http::Request<hyper::body::Incoming>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = http::Response<
                            http_body_util::combinators::BoxBody<
                                bytes::Bytes,
                                std::convert::Infallible,
                            >,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

impl RouteTable {
    /// Empty route table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            fallback: None,
            websocket: None,
        }
    }

    /// Register a route with explicit method, path, auth mode, and handler.
    #[must_use]
    pub fn route(
        mut self,
        method: Method,
        path: &str,
        auth: AuthMode,
        handler: impl Handler + 'static,
    ) -> Self {
        self.routes
            .push(RouteEntry::new(method, path, auth, handler));
        self
    }

    /// `GET` route (open).
    #[must_use]
    pub fn get(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.routes
            .push(RouteEntry::new(Method::GET, path, AuthMode::Open, handler));
        self
    }

    /// `POST` route (open).
    #[must_use]
    pub fn post(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.routes
            .push(RouteEntry::new(Method::POST, path, AuthMode::Open, handler));
        self
    }

    /// `POST` route protected by the surface session gate.
    #[must_use]
    pub fn post_session(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.routes.push(RouteEntry::new(
            Method::POST,
            path,
            AuthMode::Session,
            handler,
        ));
        self
    }

    /// `GET` route protected by gateway identity.
    #[must_use]
    pub fn get_identity(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.routes.push(RouteEntry::new(
            Method::GET,
            path,
            AuthMode::Identity,
            handler,
        ));
        self
    }

    /// `GET` route protected by the surface session gate.
    #[must_use]
    pub fn get_session(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.routes.push(RouteEntry::new(
            Method::GET,
            path,
            AuthMode::Session,
            handler,
        ));
        self
    }

    /// `POST` route protected by gateway identity.
    #[must_use]
    pub fn post_identity(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.routes.push(RouteEntry::new(
            Method::POST,
            path,
            AuthMode::Identity,
            handler,
        ));
        self
    }

    /// Merge `other` applying `auth` to every route (subtree / API module protection).
    #[must_use]
    pub fn merge_authed(mut self, auth: AuthMode, other: RouteTable) -> Self {
        for entry in other.routes {
            self.routes.push(entry.with_auth(auth));
        }
        if self.fallback.is_none() {
            self.fallback = other.fallback;
        }
        if self.websocket.is_none() {
            self.websocket = other.websocket;
        }
        self
    }

    /// Snapshot all routes for logging or parity checks.
    #[must_use]
    pub fn descriptors(&self) -> Vec<super::diff::RouteDescriptor> {
        self.routes.iter().map(RouteEntry::descriptor).collect()
    }

    /// Compare against `expected` (method + path + auth).
    #[must_use]
    pub fn diff(&self, expected: &RouteTable) -> super::diff::RouteTableDiff {
        super::diff::compare_tables(self, expected)
    }

    /// Catch-all handler when no explicit route matches (static sites, SPA shells).
    #[must_use]
    pub fn fallback(mut self, handler: impl Handler + 'static) -> Self {
        self.fallback = Some(ArcHandler::new(handler));
        self
    }

    /// WebSocket upgrade handler for `path` (hyper upgrade — use in gateway dispatch).
    #[must_use]
    pub fn websocket(
        mut self,
        path: &str,
        handler: impl Fn(
            http::Request<hyper::body::Incoming>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = http::Response<
                            http_body_util::combinators::BoxBody<
                                bytes::Bytes,
                                std::convert::Infallible,
                            >,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync
        + 'static,
    ) -> Self {
        self.websocket = Some((PathPattern::new(path), Arc::new(handler)));
        self
    }

    /// Returns the WebSocket upgrade handler when `path` matches.
    pub(crate) fn match_websocket(&self, path: &str) -> Option<&UpgradeFn> {
        let (pattern, handler) = self.websocket.as_ref()?;
        pattern.match_path(path)?;
        Some(handler)
    }

    /// Append all routes from `other` (later entries win on duplicate method+path).
    #[must_use]
    pub fn merge(mut self, other: RouteTable) -> Self {
        self.routes.extend(other.routes);
        if self.fallback.is_none() {
            self.fallback = other.fallback;
        }
        if self.websocket.is_none() {
            self.websocket = other.websocket.clone();
        }
        self
    }

    /// Number of registered routes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Whether the table has no routes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

impl std::fmt::Debug for RouteTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteTable")
            .field("len", &self.routes.len())
            .finish()
    }
}

impl RouteTable {
    /// Match method and path, then dispatch to the handler.
    ///
    /// # Errors
    /// [`HttpError::NotFound`] when no route matches; handler errors propagate.
    pub async fn dispatch(
        &self,
        method: &Method,
        path: &str,
        query: HashMap<String, String>,
        headers: http::HeaderMap,
        body: bytes::Bytes,
        gates: &DispatchGates<'_>,
    ) -> Result<Response, HttpError> {
        if let Some((entry, params)) = self.match_route(method, path) {
            run_auth(entry.auth(), gates, method, path, &query, &headers).await?;
            let ctx = RequestCtx::new(method.clone(), path, params, query, headers, body);
            return entry.handler.handle(ctx).await?.finalize();
        }
        if let Some(fallback) = &self.fallback {
            let ctx = RequestCtx::new(
                method.clone(),
                path,
                PathParams::new(),
                query,
                headers,
                body,
            );
            return fallback.handle(ctx).await?.finalize();
        }
        Err(HttpError::NotFound)
    }

    /// Dispatch without surface session or gateway identity gates.
    ///
    /// Built-in product APIs use handler-level auth; tests call this helper.
    pub async fn dispatch_open(
        &self,
        method: &Method,
        path: &str,
        query: HashMap<String, String>,
        headers: http::HeaderMap,
        body: bytes::Bytes,
    ) -> Result<Response, HttpError> {
        self.dispatch(method, path, query, headers, body, &DispatchGates::open())
            .await
    }
    fn match_route(&self, method: &Method, path: &str) -> Option<(&RouteEntry, PathParams)> {
        self.routes.iter().rev().find_map(|entry| {
            if entry.method() != method {
                return None;
            }
            entry
                .pattern()
                .match_path(path)
                .map(|params| (entry, params))
        })
    }
}

async fn run_auth(
    mode: AuthMode,
    gates: &DispatchGates<'_>,
    method: &Method,
    path: &str,
    query: &HashMap<String, String>,
    headers: &http::HeaderMap,
) -> Result<(), HttpError> {
    match mode {
        AuthMode::Open => Ok(()),
        AuthMode::Session => {
            let gate = gates.session_gate.ok_or_else(|| {
                HttpError::Internal("session route without surface SessionGate".into())
            })?;
            gate.authorize(headers).await
        }
        AuthMode::Identity => {
            let auth = gates.identity.ok_or_else(|| {
                HttpError::Internal("identity route without gateway identity hook".into())
            })?;
            let uri = build_uri(path, query);
            auth(method.clone(), uri, headers.clone()).await
        }
    }
}

fn build_uri(path: &str, query: &HashMap<String, String>) -> http::Uri {
    if query.is_empty() {
        return path.parse().unwrap_or_else(|_| http::Uri::from_static("/"));
    }
    let q = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{q}")
        .parse()
        .unwrap_or_else(|_| path.parse().unwrap_or_else(|_| http::Uri::from_static("/")))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::{Method, StatusCode};

    use super::*;
    use crate::routing::ctx::{Response, ResponseBody};

    #[tokio::test]
    async fn dispatches_get_by_path() {
        let table = RouteTable::new().get("/health", |ctx: RequestCtx| async move {
            Ok(Response::text(StatusCode::OK, ctx.path()))
        });
        let resp = table
            .dispatch_open(
                &Method::GET,
                "/health",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn captures_path_params() {
        let table = RouteTable::new().get("/items/{id}", |ctx: RequestCtx| async move {
            Ok(Response::text(
                StatusCode::OK,
                ctx.params().get("id").unwrap_or("").to_string(),
            ))
        });
        let resp = table
            .dispatch_open(
                &Method::GET,
                "/items/42",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
        if let ResponseBody::Bytes(b) = resp.body() {
            assert_eq!(b.as_ref(), b"42");
        } else {
            panic!("expected bytes body");
        }
    }

    #[tokio::test]
    async fn later_route_wins_on_duplicate() {
        let table = RouteTable::new()
            .get("/x", |_| async {
                Ok(Response::text(StatusCode::OK, "first"))
            })
            .get("/x", |_| async {
                Ok(Response::text(StatusCode::OK, "second"))
            });
        let resp = table
            .dispatch_open(
                &Method::GET,
                "/x",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect("dispatch");
        if let ResponseBody::Bytes(b) = resp.body() {
            assert_eq!(b.as_ref(), b"second");
        } else {
            panic!("expected bytes");
        }
    }

    #[tokio::test]
    async fn unknown_route_is_not_found() {
        let table = RouteTable::new();
        let err = table
            .dispatch_open(
                &Method::GET,
                "/nope",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect_err("404");
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn session_gate_rejects_unauthenticated_post() {
        use std::sync::Arc;

        use super::super::auth::{DispatchGates, SessionGate};

        let table = RouteTable::new().post_session("/orders", |_: RequestCtx| async move {
            Ok(Response::status(StatusCode::OK))
        });
        let gate = SessionGate::validate("sess", |_| async { Ok(()) });
        let gates = DispatchGates {
            session_gate: Some(&gate),
            identity: None,
        };
        let err = table
            .dispatch(
                &Method::POST,
                "/orders",
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
    async fn identity_gate_runs_before_handler() {
        use std::sync::Arc;

        use super::super::auth::{DispatchGates, IdentityAuthFn};

        let identity: IdentityAuthFn =
            Arc::new(|_, _, _| Box::pin(async { Err(HttpError::Unauthorized("nope".into())) }));
        let table = RouteTable::new().get_identity("/me", |_: RequestCtx| async move {
            Ok(Response::status(StatusCode::OK))
        });
        let gates = DispatchGates {
            session_gate: None,
            identity: Some(&identity),
        };
        let err = table
            .dispatch(
                &Method::GET,
                "/me",
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
    async fn merge_authed_applies_session_to_subtree() {
        let inner = RouteTable::new().get("/a", |_: RequestCtx| async move {
            Ok(Response::status(StatusCode::OK))
        });
        let table = RouteTable::new().merge_authed(AuthMode::Session, inner);
        assert_eq!(table.descriptors()[0].auth, AuthMode::Session);
    }
}
