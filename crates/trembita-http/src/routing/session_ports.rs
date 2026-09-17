//! Gateway session ports (B-40) — verify/issue logic separate from HTTP routes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use super::auth::SessionGate;
use super::error::HttpError;
use crate::CookieConfig;

/// Session key after successful verification (sticky routing / profile).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSession {
    /// Sticky routing key (user id, subject, room id, …).
    pub user: String,
}

/// Validates an opaque session token from a cookie (cluster-safe when stateless or shared store).
pub trait SessionVerifier: Send + Sync {
    /// Verify `token` and return the session key.
    fn verify_boxed<'a>(
        &'a self,
        token: String,
    ) -> Pin<Box<dyn Future<Output = Result<VerifiedSession, HttpError>> + Send + 'a>>;
}

/// Issues a new session token after edge identity / OIDC success.
pub trait SessionIssuer: Send + Sync {
    /// Create session token for `user` with the given TTL.
    fn issue_boxed<'a>(
        &'a self,
        user: String,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String, HttpError>> + Send + 'a>>;
}

/// Wrap a concrete verifier as [`Arc<dyn SessionVerifier>`].
#[must_use]
pub fn session_verifier<V>(verifier: V) -> Arc<dyn SessionVerifier>
where
    V: SessionVerifier + 'static,
{
    Arc::new(verifier)
}

/// Wrap a concrete issuer as [`Arc<dyn SessionIssuer>`].
#[must_use]
pub fn session_issuer<I>(issuer: I) -> Arc<dyn SessionIssuer>
where
    I: SessionIssuer + 'static,
{
    Arc::new(issuer)
}

impl SessionGate {
    /// Build a gate that delegates cookie validation to [`SessionVerifier`] (B-40).
    #[must_use]
    pub fn from_verifier(verifier: Arc<dyn SessionVerifier>, cookie: CookieConfig) -> Self {
        let name = cookie.name.clone();
        Self::validate(name, move |token| {
            let verifier = Arc::clone(&verifier);
            async move { verifier.verify_boxed(token).await.map(|_| ()) }
        })
        .with_cookie_config(cookie)
    }
}

#[cfg(test)]
mod b40_tests {
    use super::*;
    use http::HeaderMap;
    use std::future::Future;
    use std::pin::Pin;

    struct AcceptsTokPrefix;

    impl SessionVerifier for AcceptsTokPrefix {
        fn verify_boxed<'a>(
            &'a self,
            token: String,
        ) -> Pin<Box<dyn Future<Output = Result<VerifiedSession, HttpError>> + Send + 'a>> {
            Box::pin(async move {
                token
                    .strip_prefix("ok-")
                    .filter(|u| !u.is_empty())
                    .map(|u| VerifiedSession {
                        user: (*u).to_string(),
                    })
                    .ok_or_else(|| HttpError::Unauthorized("bad token".into()))
            })
        }
    }

    /// B-40 — [`SessionGate::from_verifier`] delegates cookie checks to [`SessionVerifier`].
    #[tokio::test]
    async fn b40_session_gate_from_verifier_scenarios_table() {
        let cookie = CookieConfig::from_env("TEST", "sess");
        let gate = SessionGate::from_verifier(session_verifier(AcceptsTokPrefix), cookie);

        struct Row {
            label: &'static str,
            cookie: Option<&'static str>,
            ok: bool,
        }
        let rows = [
            Row {
                label: "missing cookie",
                cookie: None,
                ok: false,
            },
            Row {
                label: "bad token",
                cookie: Some("sess=nope"),
                ok: false,
            },
            Row {
                label: "verifier accepts",
                cookie: Some("sess=ok-alice"),
                ok: true,
            },
        ];
        for row in rows {
            let mut headers = HeaderMap::new();
            if let Some(c) = row.cookie {
                headers.insert(http::header::COOKIE, http::HeaderValue::from_static(c));
            }
            let result = gate.authorize(&headers).await;
            assert_eq!(result.is_ok(), row.ok, "{}", row.label);
        }
    }
}
