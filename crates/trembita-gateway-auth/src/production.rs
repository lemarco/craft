//! Production OAuth guard defaults (B-47).

use trembita_http::HttpError;

use crate::pkce::{PkceMethod, PkcePair};
use crate::redirect::{RedirectAllowlist, RedirectError};

/// Env-backed OAuth hardening for app-owned IdP handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcProductionConfig {
    /// Exact redirect URIs permitted on authorize/callback.
    pub redirect_allowlist: RedirectAllowlist,
    /// PKCE method (default S256).
    pub pkce_method: PkceMethod,
}

impl OidcProductionConfig {
    /// Load from `TREMBITA_OAUTH_REDIRECT_ALLOWLIST` + optional `TREMBITA_OAUTH_PKCE_METHOD`.
    ///
    /// # Errors
    /// Missing/empty allowlist or unknown PKCE method env.
    pub fn from_env() -> Result<Self, OidcConfigError> {
        let redirect_allowlist = RedirectAllowlist::from_env("TREMBITA_OAUTH_REDIRECT_ALLOWLIST")
            .ok_or(OidcConfigError::MissingRedirectAllowlist)?;
        let pkce_method = std::env::var("TREMBITA_OAUTH_PKCE_METHOD")
            .ok()
            .and_then(|v| PkceMethod::parse_env(&v))
            .unwrap_or_default();
        Ok(Self {
            redirect_allowlist,
            pkce_method,
        })
    }

    /// Validate authorize-step `redirect_uri`.
    ///
    /// # Errors
    /// Maps [`RedirectError`] to HTTP 400.
    pub fn validate_redirect_uri(&self, redirect_uri: Option<&str>) -> Result<(), HttpError> {
        self.redirect_allowlist
            .check(redirect_uri)
            .map_err(redirect_to_http)
    }

    /// Validate token/callback-step PKCE (`code_verifier` vs authorize `code_challenge`).
    ///
    /// # Errors
    /// HTTP 400 when PKCE fails.
    pub fn validate_pkce(
        &self,
        verifier: Option<&str>,
        challenge: Option<&str>,
    ) -> Result<(), HttpError> {
        let Some(verifier) = verifier.filter(|s| !s.is_empty()) else {
            return Err(HttpError::BadRequest("missing code_verifier".into()));
        };
        let Some(challenge) = challenge.filter(|s| !s.is_empty()) else {
            return Err(HttpError::BadRequest("missing code_challenge".into()));
        };
        if PkcePair::verify(self.pkce_method, verifier, challenge) {
            Ok(())
        } else {
            Err(HttpError::BadRequest("invalid pkce".into()))
        }
    }
}

/// Production config parse failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OidcConfigError {
    /// `TREMBITA_OAUTH_REDIRECT_ALLOWLIST` unset or empty.
    #[error("TREMBITA_OAUTH_REDIRECT_ALLOWLIST must list at least one redirect_uri")]
    MissingRedirectAllowlist,
}

fn redirect_to_http(err: RedirectError) -> HttpError {
    match err {
        RedirectError::Missing => HttpError::BadRequest("missing redirect_uri".into()),
        RedirectError::NotAllowed => HttpError::BadRequest("redirect_uri not allowed".into()),
    }
}

/// Authorize kickoff values to pass to the IdP (store verifier server-side or in sealed cookie).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OidcAuthorizeParams {
    /// OAuth `state` — bind callback to this authorize request.
    pub state: String,
    /// PKCE `code_challenge` for the authorize redirect.
    pub code_challenge: String,
    /// PKCE method (`S256`).
    pub code_challenge_method: &'static str,
    /// Validated `redirect_uri` echo.
    pub redirect_uri: String,
}

impl OidcAuthorizeParams {
    /// Build params after redirect allowlist check; generates fresh PKCE + state.
    ///
    /// # Errors
    /// Invalid redirect URI.
    pub fn begin(
        config: &OidcProductionConfig,
        redirect_uri: &str,
    ) -> Result<(Self, PkcePair), HttpError> {
        config.validate_redirect_uri(Some(redirect_uri))?;
        let pkce = PkcePair::generate_s256();
        let state = format!("st_{}", hex::encode(getrandom_state()));
        Ok((
            Self {
                state: state.clone(),
                code_challenge: pkce.challenge.clone(),
                code_challenge_method: "S256",
                redirect_uri: redirect_uri.to_string(),
            },
            pkce,
        ))
    }
}

fn getrandom_state() -> [u8; 16] {
    let mut buf = [0u8; 16];
    getrandom::fill(&mut buf).expect("OS randomness for OAuth state");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b47_authorize_params_require_allowlisted_redirect() {
        let config = OidcProductionConfig {
            redirect_allowlist: RedirectAllowlist::parse("https://app/cb"),
            pkce_method: PkceMethod::S256,
        };
        let (params, pair) = OidcAuthorizeParams::begin(&config, "https://app/cb").expect("ok");
        assert_eq!(params.redirect_uri, "https://app/cb");
        assert!(
            config
                .validate_pkce(Some(&pair.verifier), Some(&params.code_challenge))
                .is_ok()
        );
        assert!(OidcAuthorizeParams::begin(&config, "https://evil/cb").is_err());
    }
}
