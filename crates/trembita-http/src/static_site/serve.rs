//! HTTP response assembly for static files.

use http::header;
use http::StatusCode;

use crate::routing::{HttpError, Response, ResponseBody};

use super::StaticSiteError;

/// Resolved static file ready to become an HTTP response.
#[derive(Clone, Debug)]
pub struct StaticResponse {
    /// File body.
    pub body: Vec<u8>,
    /// `Content-Type` without charset unless known.
    pub content_type: String,
    /// Optional `Content-Encoding` (`gzip`, `br`).
    pub content_encoding: Option<String>,
    /// When set, respond with `302 Found` instead of a body.
    pub redirect_to: Option<String>,
}

impl StaticResponse {
    /// Build a gateway response with cache policy derived from the request path.
    pub fn into_gateway_response(
        self,
        path: &str,
        index_cache_control: &str,
        asset_cache_control: &str,
        _spa_fallback: bool,
    ) -> Response {
        if let Some(location) = self.redirect_to {
            let mut resp = Response::status(StatusCode::FOUND);
            resp.headers_mut().insert(
                header::LOCATION,
                http::HeaderValue::from_str(&location)
                    .unwrap_or_else(|_| http::HeaderValue::from_static("/")),
            );
            return resp;
        }

        let cache = if path.ends_with("index.html") || !path.contains('.') {
            index_cache_control
        } else {
            asset_cache_control
        };

        let mut resp = Response::text(StatusCode::OK, String::new());
        *resp.body_mut() = ResponseBody::Bytes(bytes::Bytes::from(self.body));
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            http::HeaderValue::from_str(&self.content_type)
                .unwrap_or_else(|_| http::HeaderValue::from_static("application/octet-stream")),
        );
        resp.headers_mut().insert(
            header::CACHE_CONTROL,
            http::HeaderValue::from_str(cache)
                .unwrap_or_else(|_| http::HeaderValue::from_static("no-cache")),
        );
        if let Some(encoding) = self.content_encoding {
            resp.headers_mut().insert(
                header::CONTENT_ENCODING,
                http::HeaderValue::from_str(&encoding)
                    .unwrap_or_else(|_| http::HeaderValue::from_static("gzip")),
            );
        }
        resp
    }
}

/// 404 helper.
pub fn not_found() -> Response {
    Response::text(StatusCode::NOT_FOUND, "not found")
}

/// 500 helper.
pub fn internal_error(err: &StaticSiteError) -> Response {
    Response::text(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("static site error: {err}"),
    )
}
