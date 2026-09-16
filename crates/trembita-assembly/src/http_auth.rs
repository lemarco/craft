//! Minimal gateway bearer auth for cluster upgrade HTTP (no product gateway dependency).

use std::sync::Arc;

use http::header::AUTHORIZATION;
use http::{HeaderMap, Method, Uri};
use trembita_http::{AuthFn, JobsApiError};

/// Read `GATEWAY_TOKEN` or `TREMBITA_GATEWAY_TOKEN` when non-empty.
#[must_use]
pub fn gateway_token_from_env() -> Option<String> {
    ["GATEWAY_TOKEN", "TREMBITA_GATEWAY_TOKEN"]
        .into_iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Bearer auth hook when a gateway token env var is set.
#[must_use]
pub fn bearer_auth_from_env() -> Option<AuthFn> {
    let token = gateway_token_from_env()?;
    Some(Arc::new(
        move |_method: Method, _uri: Uri, headers: HeaderMap| {
            let token = token.clone();
            Box::pin(async move {
                let Some(provided) = headers
                    .get(AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
                else {
                    return Err(JobsApiError::Unauthorized("missing bearer token".into()));
                };
                if constant_time_eq(provided, &token) {
                    Ok(())
                } else {
                    Err(JobsApiError::Unauthorized("invalid bearer token".into()))
                }
            })
        },
    ))
}
