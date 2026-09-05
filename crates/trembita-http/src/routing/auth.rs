//! Route authentication modes applied before handler dispatch.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::{HeaderMap, Method, Uri};

use super::error::HttpError;

/// Async identity check hook (same contract as [`crate::AuthFn`]).
pub type IdentityAuthFn = Arc<
    dyn Fn(Method, Uri, HeaderMap) -> Pin<Box<dyn Future<Output = Result<(), HttpError>> + Send>>
        + Send
        + Sync,
>;

/// Session validation hook — supplied by the product app.
pub type SessionValidateFn = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<(), HttpError>> + Send>> + Send + Sync,
>;

/// How a route or subtree is protected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AuthMode {
    /// No auth gate — handler runs immediately after routing.
    #[default]
    Open,
    /// Requires a session cookie validated by the surface [`SessionGate`].
    Session,
    /// Runs the gateway identity hook before the handler.
    Identity,
}

/// Session cookie gate for a surface or route subtree.
#[derive(Clone)]
pub struct SessionGate {
    /// Cookie name carrying the session token.
    pub cookie_name: String,
    validate: SessionValidateFn,
}

impl std::fmt::Debug for SessionGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionGate")
            .field("cookie_name", &self.cookie_name)
            .finish_non_exhaustive()
    }
}

impl SessionGate {
    /// Declare a session gate with a validation hook.
    #[must_use]
    pub fn new(cookie_name: impl Into<String>, validate: SessionValidateFn) -> Self {
        Self {
            cookie_name: cookie_name.into(),
            validate,
        }
    }

    /// Build from a function — boxed for storage.
    #[must_use]
    pub fn validate<F, Fut>(cookie_name: impl Into<String>, f: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HttpError>> + Send + 'static,
    {
        Self::new(cookie_name, Arc::new(move |token| Box::pin(f(token))))
    }

    /// Validate the session cookie in `headers`, if present.
    pub async fn authorize(&self, headers: &HeaderMap) -> Result<(), HttpError> {
        let header = headers
            .get(http::header::COOKIE)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| HttpError::Unauthorized("No session token found".into()))?;
        let token = parse_cookie(header, &self.cookie_name)
            .ok_or_else(|| HttpError::Unauthorized("No session token found".into()))?;
        (self.validate)(token.to_string()).await
    }
}

/// Gates applied during [`super::RouteTable::dispatch`].
pub struct DispatchGates<'a> {
    /// Surface-level session gate.
    pub session_gate: Option<&'a SessionGate>,
    /// Gateway identity hook for [`AuthMode::Identity`] routes.
    pub identity: Option<&'a IdentityAuthFn>,
}

impl DispatchGates<'_> {
    /// No gates — open dispatch.
    #[must_use]
    pub fn open() -> Self {
        Self {
            session_gate: None,
            identity: None,
        }
    }
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
    use super::*;

    #[test]
    fn parse_cookie_extracts_value() {
        assert_eq!(parse_cookie("sess=abc; other=1", "sess"), Some("abc"));
        assert!(parse_cookie("other=1", "sess").is_none());
    }

    #[tokio::test]
    async fn session_gate_rejects_missing_cookie() {
        let gate = SessionGate::validate("sess", |_| async { Ok(()) });
        let err = gate.authorize(&HeaderMap::new()).await.expect_err("401");
        assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);
    }
}
