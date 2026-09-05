//! Request and response types for gateway handlers.

use std::collections::HashMap;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use serde::de::DeserializeOwned;

use super::error::HttpError;
use super::path::PathParams;

/// Incoming HTTP request context passed to [`super::Handler`](super::Handler).
#[derive(Debug, Clone)]
pub struct RequestCtx {
    method: Method,
    path: String,
    params: PathParams,
    query: HashMap<String, String>,
    headers: HeaderMap,
    body: Bytes,
}

impl RequestCtx {
    /// Build a request context for routing and handler dispatch.
    #[must_use]
    pub fn new(
        method: Method,
        path: impl Into<String>,
        params: PathParams,
        query: HashMap<String, String>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Self {
        Self {
            method,
            path: path.into(),
            params,
            query,
            headers,
            body,
        }
    }

    /// HTTP method.
    #[must_use]
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Request path (without query string).
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Path parameters from the matched route.
    #[must_use]
    pub fn params(&self) -> &PathParams {
        &self.params
    }

    /// Query parameters (first value per name).
    #[must_use]
    pub fn query(&self) -> &HashMap<String, String> {
        &self.query
    }

    /// Request headers.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Raw request body.
    #[must_use]
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Deserialize JSON body.
    ///
    /// Returns [`HttpError::InvalidJson`] with status **400** on failure — Actix-compatible,
    /// not axum's 422.
    ///
    /// # Errors
    /// [`HttpError::InvalidJson`] when the body is not valid JSON for `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, HttpError> {
        serde_json::from_slice(self.body.as_ref()).map_err(|e| {
            HttpError::InvalidJson(format!("Failed to deserialize the JSON body: {e}"))
        })
    }
}

/// Outgoing HTTP response from a gateway handler.
#[derive(Debug, Clone)]
pub struct Response {
    status: StatusCode,
    headers: HeaderMap,
    body: ResponseBody,
}

/// Response body variants.
#[derive(Debug, Clone)]
pub enum ResponseBody {
    /// No body (typical for 204 or logout).
    Empty,
    /// Raw bytes.
    Bytes(Bytes),
    /// JSON value (serialized at dispatch time).
    Json(serde_json::Value),
}

impl Response {
    /// Empty response with the given status.
    #[must_use]
    pub fn status(status: StatusCode) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: ResponseBody::Empty,
        }
    }

    /// Plain-text response.
    #[must_use]
    pub fn text(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: ResponseBody::Bytes(Bytes::from(body.into())),
        }
    }

    /// JSON response (`application/json`).
    #[must_use]
    pub fn json(status: StatusCode, value: serde_json::Value) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: ResponseBody::Json(value),
        }
    }

    /// HTTP status code.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        self.status
    }

    /// Response headers (including `Set-Cookie` when set).
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Mutable access to response headers.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Response body.
    #[must_use]
    pub fn body(&self) -> &ResponseBody {
        &self.body
    }

    /// Mutable access to the response body.
    pub fn body_mut(&mut self) -> &mut ResponseBody {
        &mut self.body
    }

    /// Serialize JSON bodies and set default `Content-Type` where missing.
    pub(crate) fn finalize(mut self) -> Result<Self, HttpError> {
        use http::header::CONTENT_TYPE;

        if let ResponseBody::Json(value) = &self.body {
            let bytes = serde_json::to_vec(value)
                .map_err(|e| HttpError::Internal(format!("json encode: {e}")))?;
            self.body = ResponseBody::Bytes(Bytes::from(bytes));
            self.headers_mut()
                .entry(CONTENT_TYPE)
                .or_insert(http::HeaderValue::from_static("application/json"));
        }
        if matches!(self.body, ResponseBody::Bytes(_)) && !self.headers.contains_key(CONTENT_TYPE)
        {
            self.headers_mut().insert(
                CONTENT_TYPE,
                http::HeaderValue::from_static("text/plain; charset=utf-8"),
            );
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn invalid_json_is_bad_request_semantics() {
        let ctx = RequestCtx::new(
            Method::POST,
            "/x",
            PathParams::new(),
            HashMap::new(),
            HeaderMap::new(),
            Bytes::from_static(b"{"),
        );
        let err = ctx.json::<serde_json::Value>().expect_err("bad json");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }
}
