//! Hyper edge service — host dispatch, CORS, and route table execution.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::header::{
    ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE,
    HOST, UPGRADE,
};
use http::{Method, Request, Response as HttpResponse, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tower::Service;

use crate::host::{is_local_dev_host, normalize_host};
use crate::routing::{
    DispatchGates, IdentityAuthFn, Response, ResponseBody, RouteTable, SessionGate,
    parse_query_string,
};

use super::cors::CorsPolicy;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Host-keyed surface wiring ready to serve.
#[derive(Clone)]
pub struct GatewayDispatch {
    hosts: Arc<HashMap<String, Arc<SurfaceDispatch>>>,
    local_dev: Option<Arc<SurfaceDispatch>>,
    identity: Option<IdentityAuthFn>,
}

#[derive(Clone)]
struct SurfaceDispatch {
    routes: RouteTable,
    cors: Option<CorsPolicy>,
    session: Option<SessionGate>,
}

impl GatewayDispatch {
    /// Build dispatch table from validated [`super::Gateway`] surfaces.
    ///
    /// # Errors
    /// [`super::GatewayBuildError`] when the gateway has no hosts or invalid surfaces.
    pub fn from_gateway(gateway: &super::Gateway) -> Result<Self, super::GatewayBuildError> {
        gateway.validate()?;
        let mut hosts = HashMap::new();
        for surface in gateway.surfaces() {
            let dispatch = Arc::new(SurfaceDispatch {
                routes: surface.route_table().clone(),
                cors: surface.cors_policy().cloned(),
                session: surface.session_gate().cloned(),
            });
            for host in surface.host_list() {
                hosts.insert(normalize_host(host), Arc::clone(&dispatch));
            }
        }
        let local_dev = gateway.dev_fallback_routes().map(|routes| {
            Arc::new(SurfaceDispatch {
                routes: routes.clone(),
                cors: gateway.dev_fallback_cors_policy().cloned(),
                session: gateway.dev_fallback_session_gate().cloned(),
            })
        });
        Ok(Self {
            hosts: Arc::new(hosts),
            local_dev,
            identity: None,
        })
    }

    /// Merge an extra host → route table (built-in product APIs).
    #[must_use]
    pub fn with_host_routes(mut self, hostname: &str, routes: RouteTable) -> Self {
        let mut map = HashMap::clone(&self.hosts);
        map.insert(
            normalize_host(hostname),
            Arc::new(SurfaceDispatch {
                routes,
                cors: None,
                session: None,
            }),
        );
        self.hosts = Arc::new(map);
        self
    }

    /// Append routes onto every registered surface (built-in APIs on all hosts).
    #[must_use]
    pub fn merge_all_surfaces(mut self, routes: &RouteTable) -> Self {
        let mut map = HashMap::new();
        for (host, surface) in self.hosts.iter() {
            let merged = surface.routes.clone().merge(routes.clone());
            map.insert(
                host.clone(),
                Arc::new(SurfaceDispatch {
                    routes: merged,
                    cors: surface.cors.clone(),
                    session: surface.session.clone(),
                }),
            );
        }
        self.hosts = Arc::new(map);
        self
    }

    /// Gateway identity hook for [`AuthMode::Identity`] routes.
    #[must_use]
    pub fn with_identity(mut self, identity: IdentityAuthFn) -> Self {
        self.identity = Some(identity);
        self
    }

    fn gates_for<'a>(&'a self, surface: &'a SurfaceDispatch) -> DispatchGates<'a> {
        DispatchGates {
            session_gate: surface.session.as_ref(),
            identity: self.identity.as_ref(),
        }
    }

    fn resolve_surface(&self, host: &str) -> Option<Arc<SurfaceDispatch>> {
        if let Some(s) = self.hosts.get(host) {
            return Some(Arc::clone(s));
        }
        if is_local_dev_host(host) {
            return self.local_dev.as_ref().map(Arc::clone);
        }
        None
    }

    #[allow(clippy::too_many_lines)]
    async fn handle(&self, req: http::Request<Incoming>) -> HttpResponse<BoxBody> {
        if is_websocket_upgrade(req.headers()) {
            let path = req.uri().path().to_string();
            let host = match req.headers().get(HOST) {
                None => return text_response(StatusCode::BAD_REQUEST, "missing Host header"),
                Some(value) => match value.to_str() {
                    Ok(raw) => normalize_host(raw),
                    Err(_) => return text_response(StatusCode::BAD_REQUEST, "invalid Host header"),
                },
            };
            let Some(surface) = self.resolve_surface(&host) else {
                return text_response(StatusCode::NOT_FOUND, &format!("unknown host: {host}"));
            };
            if let Some((handler, auth)) = surface.routes.match_websocket(&path) {
                let gates = self.gates_for(&surface);
                if let Err(err) = surface
                    .routes
                    .authorize_websocket(&path, auth, &gates, req.headers())
                    .await
                {
                    return routing_to_http_response(&err.into_http_response());
                }
                return handler(req).await;
            }
            return text_response(StatusCode::NOT_FOUND, "not found");
        }

        let (parts, body) = req.into_parts();
        let host = match parts.headers.get(HOST) {
            None => return text_response(StatusCode::BAD_REQUEST, "missing Host header"),
            Some(value) => match value.to_str() {
                Ok(raw) => normalize_host(raw),
                Err(_) => return text_response(StatusCode::BAD_REQUEST, "invalid Host header"),
            },
        };

        let Some(surface) = self.resolve_surface(&host) else {
            return text_response(StatusCode::NOT_FOUND, &format!("unknown host: {host}"));
        };

        let origin = parts
            .headers
            .get(http::header::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        if parts.method == Method::OPTIONS {
            if let (Some(cors), Some(origin)) = (&surface.cors, &origin)
                && cors.allows_origin(origin)
            {
                return cors_preflight(cors, &parts.headers);
            }
            return text_response(StatusCode::NOT_FOUND, "not found");
        }

        let path = parts.uri.path().to_string();
        if parts.method == Method::GET
            && let Some(handler) = surface.routes.match_sse(&path)
        {
            return handler().await;
        }

        let query = parse_query_string(parts.uri.query());
        let started = std::time::Instant::now();
        let body_bytes = match read_body(body, crate::routing::MAX_BODY_BYTES).await {
            Ok(b) => b,
            Err(resp) => return *resp,
        };

        let gates = self.gates_for(&surface);

        match surface
            .routes
            .dispatch(
                &parts.method,
                &path,
                query,
                parts.headers,
                body_bytes,
                &gates,
            )
            .await
        {
            Ok(resp) => {
                let status = resp.status_code();
                tracing::debug!(
                    host = %host,
                    path = %path,
                    status = %status.as_u16(),
                    latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "gateway.request"
                );
                apply_cors(
                    routing_to_http_response(
                        &resp
                            .finalize()
                            .unwrap_or_else(|e| Response::text(e.status(), e.message())),
                    ),
                    surface.cors.as_ref(),
                    origin.as_deref(),
                )
            }
            Err(err) => apply_cors(
                text_response(err.status(), err.message()),
                surface.cors.as_ref(),
                origin.as_deref(),
            ),
        }
    }
}

/// Tower/hyper service for the product gateway.
#[derive(Clone)]
pub struct GatewayService {
    dispatch: GatewayDispatch,
}

impl GatewayService {
    /// Build from a validated gateway declaration.
    ///
    /// # Errors
    /// [`super::GatewayBuildError`] when surfaces are invalid.
    pub fn build(gateway: &super::Gateway) -> Result<Self, super::GatewayBuildError> {
        Ok(Self {
            dispatch: GatewayDispatch::from_gateway(gateway)?,
        })
    }

    /// Underlying dispatch table (tests, merging built-in routes).
    #[must_use]
    pub fn dispatch(&self) -> &GatewayDispatch {
        &self.dispatch
    }

    /// Mutable access to dispatch for route merging at build time.
    pub fn dispatch_mut(&mut self) -> &mut GatewayDispatch {
        &mut self.dispatch
    }

    /// Attach gateway identity hook for [`AuthMode::Identity`] routes.
    #[must_use]
    pub fn with_identity(mut self, identity: IdentityAuthFn) -> Self {
        self.dispatch = self.dispatch.clone().with_identity(identity);
        self
    }
}

impl Service<Request<Incoming>> for GatewayService {
    type Response = HttpResponse<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        let dispatch = self.dispatch.clone();
        Box::pin(async move { Ok(dispatch.handle(req).await) })
    }
}

async fn read_body(body: Incoming, limit: usize) -> Result<Bytes, Box<HttpResponse<BoxBody>>> {
    match body.collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            if bytes.len() > limit {
                return Err(Box::new(text_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body too large",
                )));
            }
            Ok(bytes)
        }
        Err(_) => Err(Box::new(text_response(
            StatusCode::BAD_REQUEST,
            "failed to read body",
        ))),
    }
}

fn text_response(status: StatusCode, message: &str) -> HttpResponse<BoxBody> {
    let body =
        BoxBody::new(Full::new(Bytes::from(message.to_string())).map_err(|never| match never {}));
    let mut resp = HttpResponse::new(body);
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

/// Convert a routing [`Response`] into a hyper HTTP/1 response.
pub fn routing_to_http_response(response: &Response) -> HttpResponse<BoxBody> {
    let status = response.status_code();
    let body = match response.body() {
        ResponseBody::Empty => {
            BoxBody::new(http_body_util::Empty::<Bytes>::new().map_err(|never| match never {}))
        }
        ResponseBody::Bytes(b) => {
            BoxBody::new(Full::new(b.clone()).map_err(|never| match never {}))
        }
        ResponseBody::Json(_) => unreachable!("finalize converts JSON"),
    };
    let mut http = HttpResponse::new(body);
    *http.status_mut() = status;
    for (key, value) in response.headers() {
        http.headers_mut().insert(key, value.clone());
    }
    if matches!(response.body(), ResponseBody::Bytes(_))
        && let http::header::Entry::Vacant(entry) = http.headers_mut().entry(CONTENT_LENGTH)
        && let ResponseBody::Bytes(b) = response.body()
    {
        let _ = entry.insert(
            http::HeaderValue::from_str(&b.len().to_string())
                .unwrap_or(http::HeaderValue::from_static("0")),
        );
    }
    http
}

fn apply_cors(
    mut response: HttpResponse<BoxBody>,
    cors: Option<&CorsPolicy>,
    origin: Option<&str>,
) -> HttpResponse<BoxBody> {
    let Some(cors) = cors else {
        return response;
    };
    let Some(origin) = origin else {
        return response;
    };
    if !cors.allows_origin(origin) {
        return response;
    }
    if let Ok(value) = http::HeaderValue::from_str(origin) {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    if cors.credentials {
        response.headers_mut().insert(
            ACCESS_CONTROL_ALLOW_CREDENTIALS,
            http::HeaderValue::from_static("true"),
        );
    }
    response
}

fn cors_preflight(cors: &CorsPolicy, headers: &http::HeaderMap) -> HttpResponse<BoxBody> {
    let mut response = text_response(StatusCode::OK, "");
    if let Some(origin) = headers
        .get(http::header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .filter(|o| cors.allows_origin(o))
        && let Ok(value) = http::HeaderValue::from_str(origin)
    {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    response.headers_mut().insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        http::HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    response.headers_mut().insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        http::HeaderValue::from_static(
            "authorization, content-type, x-requested-with, accept, origin",
        ),
    );
    if cors.credentials {
        response.headers_mut().insert(
            ACCESS_CONTROL_ALLOW_CREDENTIALS,
            http::HeaderValue::from_static("true"),
        );
    }
    response.headers_mut().insert(
        ACCESS_CONTROL_MAX_AGE,
        http::HeaderValue::from_str(&cors.max_age.as_secs().to_string())
            .unwrap_or_else(|_| http::HeaderValue::from_static("3600")),
    );
    response
}

/// Upgrade handle for WebSocket handlers (0.4.0).
pub type UpgradeStream = TokioIo<Upgraded>;

/// Returns `true` when the request looks like a WebSocket upgrade.
#[must_use]
pub fn is_websocket_upgrade(headers: &http::HeaderMap) -> bool {
    headers
        .get(UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        && headers
            .get(CONNECTION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("upgrade"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_query() {
        let q = parse_query_string(Some("a=1&b=two"));
        assert_eq!(q.get("a").map(String::as_str), Some("1"));
        assert_eq!(q.get("b").map(String::as_str), Some("two"));
    }
}
