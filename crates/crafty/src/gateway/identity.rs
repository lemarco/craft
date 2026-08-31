//! Gateway identity extraction — user auth before sticky actor routing.

use std::any::Any;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

/// Minimal view of an incoming HTTP or WebSocket-upgrade request.
pub struct GatewayRequest<'a> {
    /// HTTP method (`GET` for WebSocket upgrade, …).
    pub method: &'a Method,
    /// Request URI (path + query).
    pub uri: &'a Uri,
    /// Request headers.
    pub headers: &'a HeaderMap,
}

impl<'a> GatewayRequest<'a> {
    /// Build from axum/http request parts.
    #[must_use]
    pub fn from_parts(method: &'a Method, uri: &'a Uri, headers: &'a HeaderMap) -> Self {
        Self {
            method,
            uri,
            headers,
        }
    }

    /// Build from any axum/http request (ignores body).
    #[must_use]
    pub fn from_http<B>(req: &'a axum::http::Request<B>) -> Self {
        Self::from_parts(req.method(), req.uri(), req.headers())
    }

    /// First query parameter value for `name` (percent-decoded when possible).
    #[must_use]
    pub fn query(&self, name: &str) -> Option<String> {
        let query = self.uri.query()?;
        form_urlencoded_query(query).find_map(|(k, v)| {
            (k == name).then(|| percent_decode(v))
        })
    }

    /// Cookie value for `name`, if present.
    #[must_use]
    pub fn cookie(&self, name: &str) -> Option<&str> {
        let header = self.headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
        parse_cookie(header, name)
    }

    /// Bearer token from `Authorization`, if present.
    #[must_use]
    pub fn bearer_token(&self) -> Option<&str> {
        let header = self.headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
        header.strip_prefix("Bearer ")
    }
}

/// Map authenticated identity to a **session key** for sticky worker pick.
pub trait SessionKey {
    /// Stable session key (user id, room id, tenant id, …).
    fn session_key(&self) -> Cow<'_, str>;
}

impl SessionKey for String {
    fn session_key(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }
}

impl SessionKey for str {
    fn session_key(&self) -> Cow<'_, str> {
        Cow::Borrowed(self)
    }
}

/// Auth failure returned by [`GatewayIdentity::extract`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    /// Missing or invalid credentials.
    #[error("unauthorized")]
    Unauthorized,
    /// Authenticated but not permitted.
    #[error("forbidden")]
    Forbidden,
    /// No [`super::GatewayOpts::identity`] configured on this gateway.
    #[error("gateway identity extractor not configured")]
    NotConfigured,
    /// Extractor internal failure.
    #[error("identity extraction failed: {0}")]
    Internal(String),
}

impl IdentityError {
    /// Suggested HTTP status for this error.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized | Self::NotConfigured => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for IdentityError {
    fn into_response(self) -> Response {
        (self.status_code(), self.to_string()).into_response()
    }
}

/// Downcast failure from [`ExtractedIdentity::require`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("identity type mismatch")]
pub struct IdentityTypeError;

/// Successful extraction — session key plus the typed identity for handlers.
pub struct ExtractedIdentity {
    session_key: String,
    inner: Box<dyn Any + Send + Sync>,
}

impl ExtractedIdentity {
    /// Sticky session key for [`super::SessionHandle::open`].
    #[must_use]
    pub fn session_key(&self) -> &str {
        &self.session_key
    }

    /// Downcast to your identity type.
    #[must_use]
    pub fn get<I: 'static>(&self) -> Option<&I> {
        self.inner.downcast_ref()
    }

    /// Typed identity reference.
    ///
    /// # Errors
    /// Returns [`IdentityTypeError`] when the stored type does not match `I`.
    pub fn require<I: 'static>(&self) -> Result<&I, IdentityTypeError> {
        self.get::<I>().ok_or(IdentityTypeError)
    }

    /// Consume and downcast to your identity type.
    #[must_use]
    pub fn into_inner<I: 'static>(self) -> Option<I> {
        self.inner.downcast().ok().map(|b| *b)
    }
}

/// User-supplied identity extractor (JWT, cookie→DB, API key, …).
pub trait GatewayIdentity: Send + Sync + 'static {
    /// Your identity type after successful auth.
    type Identity: Send + Sync + 'static;

    /// Validate the request and return identity, or an auth error.
    fn extract<'a>(
        &'a self,
        req: &'a GatewayRequest<'_>,
    ) -> impl Future<Output = Result<Self::Identity, IdentityError>> + Send + 'a;
}

pub(crate) trait DynGatewayIdentity: Send + Sync {
    fn extract_dyn<'a>(
        &'a self,
        req: &'a GatewayRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ExtractedIdentity, IdentityError>> + Send + 'a>>;
}

struct IdentityAdapter<I> {
    inner: I,
    session_key: Arc<dyn Fn(&dyn Any) -> String + Send + Sync>,
}

impl<I> DynGatewayIdentity for IdentityAdapter<I>
where
    I: GatewayIdentity,
{
    fn extract_dyn<'a>(
        &'a self,
        req: &'a GatewayRequest<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<ExtractedIdentity, IdentityError>> + Send + 'a>> {
        let session_key_fn = Arc::clone(&self.session_key);
        Box::pin(async move {
            let identity = self.inner.extract(req).await?;
            let boxed: Box<dyn Any + Send + Sync> = Box::new(identity);
            Ok(ExtractedIdentity {
                session_key: session_key_fn(boxed.as_ref()),
                inner: boxed,
            })
        })
    }
}

pub(crate) fn erase_identity<I>(extractor: I) -> Arc<dyn DynGatewayIdentity>
where
    I: GatewayIdentity,
    I::Identity: SessionKey,
{
    let session_key = Arc::new(|any: &dyn Any| {
        any.downcast_ref::<I::Identity>()
            .expect("identity type mismatch")
            .session_key()
            .into_owned()
    });
    Arc::new(IdentityAdapter {
        inner: extractor,
        session_key,
    })
}

pub(crate) fn erase_identity_mapped<I, F>(extractor: I, session_key: F) -> Arc<dyn DynGatewayIdentity>
where
    I: GatewayIdentity,
    I::Identity: 'static,
    F: Fn(&I::Identity) -> String + Send + Sync + 'static,
{
    let session_key = Arc::new(move |any: &dyn Any| {
        session_key(
            any.downcast_ref::<I::Identity>()
                .expect("identity type mismatch"),
        )
    });
    Arc::new(IdentityAdapter {
        inner: extractor,
        session_key,
    })
}

/// Demo extractor: `?user=` session key + optional `?token=` ([`GatewayTokenIdentity`]).
#[derive(Debug, Clone, Default)]
pub struct GatewayTokenIdentity {
    env_var: String,
}

impl GatewayTokenIdentity {
    /// Require `?token=` matching `GATEWAY_TOKEN` (or `env_var`).
    #[must_use]
    pub fn new(env_var: impl Into<String>) -> Self {
        Self {
            env_var: env_var.into(),
        }
    }

    /// Default: `GATEWAY_TOKEN` environment variable.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new("GATEWAY_TOKEN")
    }
}

impl GatewayIdentity for GatewayTokenIdentity {
    type Identity = String;

    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        let user = req.query("user").ok_or(IdentityError::Unauthorized)?;
        let token = req.query("token").ok_or(IdentityError::Unauthorized)?;
        let expected = std::env::var(&self.env_var).unwrap_or_default();
        if expected.is_empty() || token == expected {
            Ok(user)
        } else {
            Err(IdentityError::Unauthorized)
        }
    }
}

fn form_urlencoded_query(query: &str) -> impl Iterator<Item = (&str, &str)> {
    query.split('&').filter_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        Some((k, v))
    })
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_cookie<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    header.split(';').find_map(|part| {
        let part = part.trim();
        part.strip_prefix(&prefix)
    })
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, Method, Uri};

    use super::*;

    #[test]
    fn gateway_request_query_and_cookie() {
        let uri: Uri = "/ws?user=alice&token=secret".parse().expect("uri");
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_static("session_id=abc; other=1"),
        );
        let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
        assert_eq!(req.query("user").as_deref(), Some("alice"));
        assert_eq!(req.cookie("session_id"), Some("abc"));
    }

    #[test]
    fn query_percent_decode() {
        let uri: Uri = "/ws?user=alice%40example.com".parse().expect("uri");
        let headers = HeaderMap::new();
        let req = GatewayRequest::from_parts(&Method::GET, &uri, &headers);
        assert_eq!(req.query("user").as_deref(), Some("alice@example.com"));
    }
}
