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
use crate::routing::{HttpError, RequestCtx, Response, ResponseBody, RouteTable};

use super::Surface;
use super::cors::CorsPolicy;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Host-keyed surface wiring ready to serve.
#[derive(Clone)]
pub struct GatewayDispatch {
    hosts: Arc<HashMap<String, Arc<SurfaceDispatch>>>,
    local_dev: Option<Arc<SurfaceDispatch>>,
    is_production: bool,
}

#[derive(Clone)]
struct SurfaceDispatch {
    routes: RouteTable,
    cors: Option<CorsPolicy>,
}

impl GatewayDispatch {
    /// Build dispatch table from validated [`super::Gateway`] surfaces.
    pub fn from_gateway(gateway: &super::Gateway) -> Result<Self, super::GatewayBuildError> {
        gateway.validate()?;
        let mut hosts = HashMap::new();
        for surface in gateway.surfaces() {
            let dispatch = Arc::new(SurfaceDispatch {
                routes: surface.route_table().clone(),
                cors: surface.cors_policy().cloned(),
            });
            for host in surface.host_list() {
                hosts.insert(normalize_host(host), Arc::clone(&dispatch));
            }
        }
        let local_dev = gateway.dev_fallback_routes().map(|routes| {
            Arc::new(SurfaceDispatch {
                routes: routes.clone(),
                cors: None,
            })
        });
        Ok(Self {
            hosts: Arc::new(hosts),
            local_dev,
            is_production: gateway.is_production(),
        })
    }

    /// Merge an extra host → route table (built-in product APIs).
    #[must_use]
    pub fn with_host_routes(mut self, hostname: &str, routes: RouteTable) -> Self {
        let mut map = HashMap::clone(&self.hosts);
        map.insert(
            normalize_host(hostname),
            Arc::new(SurfaceDispatch { routes, cors: None }),
        );
        self.hosts = Arc::new(map);
        self
    }

    /// Append routes onto every registered surface (built-in APIs on all hosts).
    #[must_use]
    pub fn merge_all_surfaces(mut self, routes: RouteTable) -> Self {
        let mut map = HashMap::new();
        for (host, surface) in self.hosts.iter() {
            let merged = surface.routes.clone().merge(routes.clone());
            map.insert(
                host.clone(),
                Arc::new(SurfaceDispatch {
                    routes: merged,
                    cors: surface.cors.clone(),
                }),
            );
        }
        self.hosts = Arc::new(map);
        self
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
            let surface = match self.resolve_surface(&host) {
                Some(s) => s,
                None => {
                    return text_response(StatusCode::NOT_FOUND, &format!("unknown host: {host}"));
                }
            };
            if let Some(handler) = surface.routes.match_websocket(&path) {
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

        let surface = match self.resolve_surface(&host) {
            Some(s) => s,
            None => return text_response(StatusCode::NOT_FOUND, &format!("unknown host: {host}")),
        };

        let origin = parts
            .headers
            .get(http::header::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        if parts.method == Method::OPTIONS {
            if let (Some(cors), Some(origin)) = (&surface.cors, &origin) {
                if cors.allows_origin(origin) {
                    return cors_preflight(cors, &parts.headers);
                }
            }
            return text_response(StatusCode::NOT_FOUND, "not found");
        }

        let path = parts.uri.path().to_string();
        let query = parse_query(parts.uri.query());
        let body_bytes = match read_body(body, crate::routing::MAX_BODY_BYTES).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };

        match surface
            .routes
            .dispatch(&parts.method, &path, query, parts.headers, body_bytes)
            .await
        {
            Ok(resp) => apply_cors(
                to_http_response(
                    resp.finalize()
                        .unwrap_or_else(|e| Response::text(e.status(), e.message())),
                ),
                surface.cors.as_ref(),
                origin.as_deref(),
            ),
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

fn parse_query(raw: Option<&str>) -> HashMap<String, String> {
    let Some(raw) = raw else {
        return HashMap::new();
    };
    raw.split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

async fn read_body(body: Incoming, limit: usize) -> Result<Bytes, HttpResponse<BoxBody>> {
    match body.collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            if bytes.len() > limit {
                return Err(text_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body too large",
                ));
            }
            Ok(bytes)
        }
        Err(_) => Err(text_response(
            StatusCode::BAD_REQUEST,
            "failed to read body",
        )),
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

fn to_http_response(response: Response) -> HttpResponse<BoxBody> {
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
    for (key, value) in response.headers().iter() {
        http.headers_mut().insert(key, value.clone());
    }
    if matches!(response.body(), ResponseBody::Bytes(_)) {
        if let http::header::Entry::Vacant(entry) = http.headers_mut().entry(CONTENT_LENGTH) {
            if let ResponseBody::Bytes(b) = response.body() {
                let _ = entry.insert(
                    http::HeaderValue::from_str(&b.len().to_string())
                        .unwrap_or(http::HeaderValue::from_static("0")),
                );
            }
        }
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
    use http::StatusCode;

    #[test]
    fn parse_query_string() {
        let q = parse_query(Some("a=1&b=two"));
        assert_eq!(q.get("a").map(String::as_str), Some("1"));
        assert_eq!(q.get("b").map(String::as_str), Some("two"));
    }
}
