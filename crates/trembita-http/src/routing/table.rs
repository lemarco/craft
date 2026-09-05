//! Route table — declarative method + path → handler mapping.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::Method;

use super::auth::AuthMode;
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
    ) -> Result<Response, HttpError> {
        if let Some((entry, params)) = self.match_route(method, path) {
            let _auth = entry.auth();
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
            .dispatch(
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
            .dispatch(
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
            .dispatch(
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
            .dispatch(
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
}
