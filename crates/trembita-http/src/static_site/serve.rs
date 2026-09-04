//! HTTP response assembly for static files.

use axum::body::Body;
use axum::http::{Response, StatusCode, header};
use axum::response::IntoResponse;

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
    /// Build an Axum response with cache policy derived from the request path.
    pub fn into_response(
        self,
        path: &str,
        index_cache_control: &str,
        asset_cache_control: &str,
        _spa_fallback: bool,
    ) -> Response<Body> {
        if let Some(location) = self.redirect_to {
            return Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, location)
                .body(Body::empty())
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }

        let cache = if path.ends_with("index.html") || !path.contains('.') {
            index_cache_control
        } else {
            asset_cache_control
        };

        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, self.content_type)
            .header(header::CACHE_CONTROL, cache);

        if let Some(encoding) = self.content_encoding {
            builder = builder.header(header::CONTENT_ENCODING, encoding);
        }

        builder
            .body(Body::from(self.body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

/// 404 helper.
pub fn not_found() -> Response<Body> {
    StatusCode::NOT_FOUND.into_response()
}

/// 500 helper.
pub fn internal_error(err: &StaticSiteError) -> Response<Body> {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("static site error: {err}"),
    )
        .into_response()
}
