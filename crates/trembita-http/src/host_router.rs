//! Virtual-host dispatch for a single gateway listen port.
//!
//! Axum has no built-in `Host`-based routing. Without a shared helper every app
//! reimplements the same fallback layer and often leaves a dev catch-all enabled
//! in production. [`HostRouter`] matches **exact hostnames** and returns **404**
//! for unknown hosts unless you opt in to [`HostRouter::local_dev_fallback`] or
//! [`HostRouter::unmatched_fallback`].
//!
//! Register sub-routers **after** [`.with_state`](Router::with_state) so each
//! host router is a complete [`Router<()>`].

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use axum::response::IntoResponse;
use tower::ServiceExt;

/// Normalize an HTTP `Host` header to a lowercase hostname without port.
///
/// `API.Example.COM:443` → `api.example.com`, `[::1]:8080` → `::1`.
#[must_use]
pub fn normalize_host(raw: &str) -> String {
    let raw = raw.trim().to_ascii_lowercase();
    if let Some(stripped) = raw.strip_prefix('[')
        && let Some(end) = stripped.find(']')
    {
        return stripped[..end].to_string();
    }
    raw.split(':').next().unwrap_or(&raw).to_string()
}

/// Hostnames that [`HostRouter::local_dev_fallback`] recognizes.
#[must_use]
pub fn is_local_dev_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

/// Builder for host-keyed Axum routers sharing one listen socket.
pub struct HostRouter {
    hosts: HashMap<String, Router>,
    local_dev: Option<Router>,
    unmatched: Option<Router>,
}

impl fmt::Debug for HostRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostRouter")
            .field("hosts", &self.hosts.keys().collect::<Vec<_>>())
            .field("local_dev", &self.local_dev.as_ref().map(|_| "<router>"))
            .field("unmatched", &self.unmatched.as_ref().map(|_| "<router>"))
            .finish()
    }
}

impl Default for HostRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl HostRouter {
    /// Empty router map — add hosts with [`.host`](Self::host) before [`.build`](Self::build).
    #[must_use]
    pub fn new() -> Self {
        Self {
            hosts: HashMap::new(),
            local_dev: None,
            unmatched: None,
        }
    }

    /// Route `hostname` (port ignored on incoming requests) to `router`.
    ///
    /// `hostname` is normalized the same way as request `Host` headers ([`normalize_host`]).
    /// Call [`.with_state`](Router::with_state) on `router` before passing it here.
    #[must_use]
    pub fn host(mut self, hostname: impl AsRef<str>, router: Router) -> Self {
        self.hosts.insert(normalize_host(hostname.as_ref()), router);
        self
    }

    /// Serve loopback hostnames only (`localhost`, `127.0.0.1`, `::1`).
    ///
    /// Does **not** catch arbitrary unknown hosts — production misconfiguration
    /// still returns 404 instead of silently hitting dev routes.
    #[must_use]
    pub fn local_dev_fallback(mut self, router: Router) -> Self {
        self.local_dev = Some(router);
        self
    }

    /// Catch-all for any host not listed in [`.host`](Self::host).
    ///
    /// Prefer explicit per-host registration in production; use
    /// [`.local_dev_fallback`](Self::local_dev_fallback) for local iteration.
    #[must_use]
    pub fn unmatched_fallback(mut self, router: Router) -> Self {
        self.unmatched = Some(router);
        self
    }

    /// Route `hostname` to a [`StaticSite`] router (convenience for [`.host`](Self::host)).
    #[must_use]
    pub fn static_site(self, hostname: impl AsRef<str>, site: crate::StaticSite) -> Self {
        self.host(hostname, site.router())
    }

    /// Merge into one [`Router`] that dispatches by the `Host` header.
    pub fn build(self) -> Router {
        let dispatch = HostDispatch {
            hosts: Arc::new(self.hosts),
            local_dev: self.local_dev,
            unmatched: self.unmatched,
        };
        Router::new().fallback(move |req: Request<Body>| {
            let dispatch = dispatch.clone();
            async move { dispatch.serve(req).await }
        })
    }
}

#[derive(Clone)]
struct HostDispatch {
    hosts: Arc<HashMap<String, Router>>,
    local_dev: Option<Router>,
    unmatched: Option<Router>,
}

impl HostDispatch {
    async fn serve(self, req: Request<Body>) -> Response<Body> {
        let host = match req.headers().get(header::HOST) {
            None => {
                return (StatusCode::BAD_REQUEST, "missing Host header").into_response();
            }
            Some(value) => match value.to_str() {
                Ok(raw) => normalize_host(raw),
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "invalid Host header").into_response();
                }
            },
        };

        let router = if let Some(router) = self.hosts.get(&host) {
            router
        } else if is_local_dev_host(&host) {
            match &self.local_dev {
                Some(router) => router,
                None => {
                    return unknown_host(&host);
                }
            }
        } else if let Some(router) = &self.unmatched {
            router
        } else {
            return unknown_host(&host);
        };

        match router.clone().oneshot(req).await {
            Ok(response) => response,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

fn unknown_host(host: &str) -> Response<Body> {
    (StatusCode::NOT_FOUND, format!("unknown host: {host}")).into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    async fn hello() -> &'static str {
        "hello"
    }

    async fn dev() -> &'static str {
        "dev"
    }

    #[test]
    fn normalize_strips_port_and_lowercases() {
        assert_eq!(normalize_host("API.Example.COM:443"), "api.example.com");
        assert_eq!(normalize_host("[::1]:8080"), "::1");
        assert_eq!(normalize_host("localhost"), "localhost");
    }

    #[tokio::test]
    async fn strict_mode_returns_404_for_unknown_host() {
        let app = HostRouter::new()
            .host("api.example.com", Router::new().route("/", get(hello)))
            .build();

        let req = Request::builder()
            .uri("/")
            .header(header::HOST, "other.example.com")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn routes_matching_host() {
        let app = HostRouter::new()
            .host("api.example.com", Router::new().route("/", get(hello)))
            .build();

        let req = Request::builder()
            .uri("/")
            .header(header::HOST, "api.example.com:8080")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn local_dev_fallback_only_for_loopback() {
        let app = HostRouter::new()
            .host("api.example.com", Router::new().route("/", get(hello)))
            .local_dev_fallback(Router::new().route("/", get(dev)))
            .build();

        let local = Request::builder()
            .uri("/")
            .header(header::HOST, "127.0.0.1:8080")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(local).await.unwrap().status(),
            StatusCode::OK
        );

        let unknown = Request::builder()
            .uri("/")
            .header(header::HOST, "staging.example.com")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(unknown).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn missing_host_is_bad_request() {
        let app = HostRouter::new()
            .host("api.example.com", Router::new().route("/", get(hello)))
            .build();

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
