//! Dev OIDC callback — maps `?user=` to subject (no network IdP).

use std::sync::Arc;
use std::time::Duration;

use trembita_http::{HttpError, RequestCtx, Response, SessionGate, SessionIssuer};

use crate::issue_gateway_session;

/// Stand-in for an OIDC authorization-code handler in local demos.
#[derive(Clone)]
pub struct DevOidcCallback {
    issuer: Arc<dyn SessionIssuer>,
    gate: SessionGate,
    ttl: Duration,
}

impl DevOidcCallback {
    /// Build dev callback wiring.
    #[must_use]
    pub fn new(issuer: Arc<dyn SessionIssuer>, gate: SessionGate, ttl: Duration) -> Self {
        Self { issuer, gate, ttl }
    }

    /// `GET /oauth/callback?user=…` — issue cluster session cookie.
    ///
    /// # Errors
    /// Missing/invalid user or issuer failure.
    pub async fn handle(&self, ctx: RequestCtx) -> Result<Response, HttpError> {
        let user = ctx
            .query_param("user")
            .ok_or_else(|| HttpError::BadRequest("missing user".into()))?;
        if user.is_empty() || user.len() > 256 {
            return Err(HttpError::BadRequest("invalid user".into()));
        }
        issue_gateway_session(user, Arc::clone(&self.issuer), &self.gate, self.ttl).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;
    use trembita_http::{CookieConfig, SessionGate, SessionIssuer};

    struct StaticIssuer;

    impl SessionIssuer for StaticIssuer {
        fn issue_boxed<'a>(
            &'a self,
            user: String,
            _ttl: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<String, HttpError>> + Send + 'a>> {
            Box::pin(async move { Ok(format!("tok-{user}")) })
        }
    }

    #[tokio::test]
    async fn b40_dev_oidc_callback_issues_session_cookie() {
        let cookie = CookieConfig::from_env("TEST", "sess");
        let gate = SessionGate::from_verifier(
            trembita_http::session_verifier(StaticVerifier),
            cookie.clone(),
        );
        let cb = DevOidcCallback::new(Arc::new(StaticIssuer), gate, Duration::from_secs(60));
        let mut query = std::collections::HashMap::new();
        query.insert("user".into(), "oidc-alice".into());
        let ctx = RequestCtx::new(
            http::Method::GET,
            "/oauth/callback",
            trembita_http::PathParams::new(),
            query,
            http::HeaderMap::new(),
            bytes::Bytes::new(),
        );
        let resp = cb.handle(ctx).await.expect("callback");
        assert_eq!(resp.status_code(), http::StatusCode::OK);
        assert!(
            resp.headers()
                .get(http::header::SET_COOKIE)
                .is_some_and(|v| v.to_str().unwrap().starts_with("sess="))
        );
    }

    struct StaticVerifier;

    impl trembita_http::SessionVerifier for StaticVerifier {
        fn verify_boxed<'a>(
            &'a self,
            token: String,
        ) -> Pin<
            Box<dyn Future<Output = Result<trembita_http::VerifiedSession, HttpError>> + Send + 'a>,
        > {
            Box::pin(async move {
                Ok(trembita_http::VerifiedSession {
                    user: token.strip_prefix("tok-").unwrap_or("").to_string(),
                })
            })
        }
    }

    #[tokio::test]
    async fn b40_dev_oidc_callback_rejects_invalid_user_scenarios_table() {
        let cookie = CookieConfig::from_env("TEST", "sess");
        let gate =
            SessionGate::from_verifier(trembita_http::session_verifier(StaticVerifier), cookie);
        let cb = DevOidcCallback::new(Arc::new(StaticIssuer), gate, Duration::from_secs(60));

        struct Row {
            label: &'static str,
            user: Option<String>,
        }
        let rows = [
            Row {
                label: "missing user query",
                user: None,
            },
            Row {
                label: "empty user",
                user: Some(String::new()),
            },
            Row {
                label: "overlong user",
                user: Some("x".repeat(257)),
            },
        ];
        for row in rows {
            let mut query = std::collections::HashMap::new();
            if let Some(u) = row.user {
                query.insert("user".into(), u);
            }
            let ctx = RequestCtx::new(
                http::Method::GET,
                "/oauth/callback",
                trembita_http::PathParams::new(),
                query,
                http::HeaderMap::new(),
                bytes::Bytes::new(),
            );
            let err = cb.handle(ctx).await.expect_err(row.label);
            assert!(
                matches!(err, HttpError::BadRequest(_)),
                "{}: {:?}",
                row.label,
                err
            );
        }
    }
}
