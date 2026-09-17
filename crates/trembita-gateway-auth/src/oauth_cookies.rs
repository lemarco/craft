//! Short-lived OAuth pending cookies (PKCE verifier + state) for authorize/callback binding.

use http::header::SET_COOKIE;
use trembita_http::Response;

/// Cookie holding OAuth `state` until callback.
pub const OAUTH_STATE_COOKIE: &str = "oauth_state";
/// Cookie holding PKCE `code_verifier` until callback.
pub const OAUTH_PKCE_COOKIE: &str = "oauth_pkce_verifier";

const PENDING_MAX_AGE_SECS: i64 = 600;

/// Attach HttpOnly pending cookies for the authorize step (Path `/oauth`).
pub fn append_oauth_pending_cookies(response: &mut Response, state: &str, pkce_verifier: &str) {
    for value in [
        pending_cookie(OAUTH_STATE_COOKIE, state),
        pending_cookie(OAUTH_PKCE_COOKIE, pkce_verifier),
    ] {
        response
            .headers_mut()
            .append(SET_COOKIE, value.parse().expect("set-cookie"));
    }
}

fn pending_cookie(name: &str, value: &str) -> String {
    format!("{name}={value}; Max-Age={PENDING_MAX_AGE_SECS}; Path=/oauth; HttpOnly; SameSite=Lax")
}
