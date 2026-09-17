//! Authorize kickoff — redirect allowlist + PKCE (B-47).

use trembita_http::{HttpError, RequestCtx, Response};

use crate::oauth_cookies::append_oauth_pending_cookies;
use crate::production::{OidcAuthorizeParams, OidcProductionConfig};

/// Dev authorize endpoint backing hardened OAuth demos.
#[derive(Clone)]
pub struct DevOidcAuthorize {
    config: OidcProductionConfig,
}

impl DevOidcAuthorize {
    /// Wire production OAuth guards.
    #[must_use]
    pub fn new(config: OidcProductionConfig) -> Self {
        Self { config }
    }

    /// `GET /oauth/start?redirect_uri=…` — validate redirect, mint PKCE + state, set pending cookies.
    ///
    /// # Errors
    /// Invalid/missing redirect URI.
    pub async fn handle(&self, ctx: RequestCtx) -> Result<Response, HttpError> {
        let redirect_uri = ctx
            .query_param("redirect_uri")
            .ok_or_else(|| HttpError::BadRequest("missing redirect_uri".into()))?;
        let (params, pkce) = OidcAuthorizeParams::begin(&self.config, redirect_uri)?;
        let mut body =
            serde_json::to_value(&params).map_err(|e| HttpError::Internal(e.to_string()))?;
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "callback_hint".into(),
                serde_json::json!(format!(
                    "/oauth/callback?user={{subject}}&redirect_uri={}&state={}&code_challenge={}",
                    params.redirect_uri, params.state, params.code_challenge
                )),
            );
        }
        let mut resp = Response::json(http::StatusCode::OK, body);
        append_oauth_pending_cookies(&mut resp, &params.state, &pkce.verifier);
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pkce::PkceMethod;
    use crate::redirect::RedirectAllowlist;

    #[tokio::test]
    async fn b47_dev_authorize_sets_pending_cookies_and_json() {
        let config = OidcProductionConfig {
            redirect_allowlist: RedirectAllowlist::parse("https://app/cb"),
            pkce_method: PkceMethod::S256,
        };
        let auth = DevOidcAuthorize::new(config);
        let mut query = std::collections::HashMap::new();
        query.insert("redirect_uri".into(), "https://app/cb".into());
        let ctx = RequestCtx::new(
            http::Method::GET,
            "/oauth/start",
            trembita_http::PathParams::new(),
            query,
            http::HeaderMap::new(),
            bytes::Bytes::new(),
        );
        let resp = auth.handle(ctx).await.expect("start");
        assert_eq!(resp.status_code(), http::StatusCode::OK);
        let cookies: Vec<String> = resp
            .headers()
            .get_all(http::header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok().map(String::from))
            .collect();
        assert!(cookies.iter().any(|c| c.starts_with("oauth_state=st_")));
        assert!(
            cookies
                .iter()
                .any(|c| c.starts_with("oauth_pkce_verifier="))
        );
    }
}
