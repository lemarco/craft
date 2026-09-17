//! Gateway auth helpers — external IdP → [`SessionIssuer`] (B-40).

#![deny(unsafe_code)]

mod authorize;
mod dev_oidc;
mod oauth_cookies;
mod pkce;
mod production;
mod redirect;

use std::sync::Arc;
use std::time::Duration;

pub use authorize::DevOidcAuthorize;
pub use dev_oidc::DevOidcCallback;
use http::StatusCode;
pub use oauth_cookies::{OAUTH_PKCE_COOKIE, OAUTH_STATE_COOKIE};
pub use pkce::{PkceMethod, PkcePair};
pub use production::{OidcAuthorizeParams, OidcConfigError, OidcProductionConfig};
pub use redirect::{RedirectAllowlist, RedirectError};
use trembita_http::{HttpError, Response, SessionGate, SessionIssuer};

/// After IdP success, mint product session cookie via [`SessionIssuer`].
///
/// # Errors
/// Issuer failure or invalid `Set-Cookie`.
pub async fn issue_gateway_session(
    user: &str,
    issuer: Arc<dyn SessionIssuer>,
    gate: &SessionGate,
    ttl: Duration,
) -> Result<Response, HttpError> {
    let token = issuer.issue_boxed(user.to_string(), ttl).await?;
    let mut resp = Response::json(StatusCode::OK, serde_json::json!({ "user": user }));
    gate.set_session_cookie(&mut resp, &token)?;
    Ok(resp)
}

#[cfg(test)]
mod b40_tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use trembita_http::{CookieConfig, SessionGate, SessionIssuer};

    struct StaticIssuer;

    impl SessionIssuer for StaticIssuer {
        fn issue_boxed<'a>(
            &'a self,
            user: String,
            _ttl: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<String, HttpError>> + Send + 'a>> {
            Box::pin(async move { Ok(format!("static-{user}")) })
        }
    }

    #[tokio::test]
    async fn b40_issue_gateway_session_json_body_and_set_cookie() {
        let cookie = CookieConfig::from_env("TEST", "sess");
        let gate =
            SessionGate::from_verifier(trembita_http::session_verifier(StaticVerifier), cookie);
        let resp = issue_gateway_session(
            "gw-user",
            Arc::new(StaticIssuer),
            &gate,
            Duration::from_secs(300),
        )
        .await
        .expect("issue");
        assert_eq!(resp.status_code(), StatusCode::OK);
        match resp.body() {
            trembita_http::ResponseBody::Json(v) => {
                assert_eq!(v["user"], "gw-user");
            }
            other => panic!("expected json body, got {other:?}"),
        }
        assert!(
            resp.headers()
                .get(http::header::SET_COOKIE)
                .is_some_and(|v| v.to_str().unwrap().starts_with("sess=static-gw-user"))
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
                    user: token.strip_prefix("static-").unwrap_or("").to_string(),
                })
            })
        }
    }
}
