//! Dev OIDC callback — maps `?user=` to subject (no network IdP).

use std::sync::Arc;
use std::time::Duration;

use trembita_http::{HttpError, RequestCtx, Response, SessionGate, SessionIssuer};

use crate::issue_gateway_session;
use crate::oauth_cookies::{OAUTH_PKCE_COOKIE, OAUTH_STATE_COOKIE};
use crate::production::OidcProductionConfig;

/// Stand-in for an OIDC authorization-code handler in local demos.
#[derive(Clone)]
pub struct DevOidcCallback {
    issuer: Arc<dyn SessionIssuer>,
    gate: SessionGate,
    ttl: Duration,
    production: Option<OidcProductionConfig>,
}

impl DevOidcCallback {
    /// Build dev callback wiring.
    #[must_use]
    pub fn new(issuer: Arc<dyn SessionIssuer>, gate: SessionGate, ttl: Duration) -> Self {
        Self {
            issuer,
            gate,
            ttl,
            production: None,
        }
    }

    /// Enable redirect allowlist + PKCE (S256) checks (B-47).
    #[must_use]
    pub fn with_production(mut self, config: OidcProductionConfig) -> Self {
        self.production = Some(config);
        self
    }

    /// `GET /oauth/callback?user=…` — issue cluster session cookie.
    ///
    /// With [`Self::with_production`], also requires allowlisted `redirect_uri`, matching
    /// `state` cookie/query, and PKCE verifier cookie vs `code_challenge` query.
    ///
    /// # Errors
    /// Missing/invalid user or issuer failure.
    pub async fn handle(&self, ctx: RequestCtx) -> Result<Response, HttpError> {
        if let Some(config) = &self.production {
            config.validate_redirect_uri(ctx.query_param("redirect_uri"))?;
            let state = ctx
                .query_param("state")
                .ok_or_else(|| HttpError::BadRequest("missing state".into()))?;
            let challenge = ctx
                .query_param("code_challenge")
                .ok_or_else(|| HttpError::BadRequest("missing code_challenge".into()))?;
            let cookie_state = ctx
                .cookie(OAUTH_STATE_COOKIE)
                .ok_or_else(|| HttpError::BadRequest("missing oauth state cookie".into()))?;
            let verifier = ctx
                .cookie(OAUTH_PKCE_COOKIE)
                .ok_or_else(|| HttpError::BadRequest("missing pkce cookie".into()))?;
            if state != cookie_state {
                return Err(HttpError::BadRequest("state mismatch".into()));
            }
            config.validate_pkce(Some(verifier), Some(challenge))?;
        }
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
    async fn b47_dev_oidc_callback_production_pkce_and_state() {
        use crate::oauth_cookies::{OAUTH_PKCE_COOKIE, OAUTH_STATE_COOKIE};
        use crate::pkce::PkceMethod;
        use crate::production::OidcProductionConfig;
        use crate::redirect::RedirectAllowlist;

        let cookie = CookieConfig::from_env("TEST", "sess");
        let gate =
            SessionGate::from_verifier(trembita_http::session_verifier(StaticVerifier), cookie);
        let config = OidcProductionConfig {
            redirect_allowlist: RedirectAllowlist::parse("https://app/cb"),
            pkce_method: PkceMethod::S256,
        };
        let pair = crate::pkce::PkcePair::generate_s256();
        let state = "st_test";
        let cb = DevOidcCallback::new(Arc::new(StaticIssuer), gate, Duration::from_secs(60))
            .with_production(config);
        let mut headers = http::HeaderMap::new();
        headers.append(
            http::header::COOKIE,
            format!(
                "{OAUTH_STATE_COOKIE}={state}; {OAUTH_PKCE_COOKIE}={}",
                pair.verifier
            )
            .parse()
            .unwrap(),
        );
        let mut query = std::collections::HashMap::new();
        query.insert("user".into(), "alice".into());
        query.insert("redirect_uri".into(), "https://app/cb".into());
        query.insert("state".into(), state.into());
        query.insert("code_challenge".into(), pair.challenge.clone());
        let ctx = RequestCtx::new(
            http::Method::GET,
            "/oauth/callback",
            trembita_http::PathParams::new(),
            query,
            headers,
            bytes::Bytes::new(),
        );
        let resp = cb.handle(ctx).await.expect("hardened callback");
        assert_eq!(resp.status_code(), http::StatusCode::OK);
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
