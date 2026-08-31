//! Shared [`GatewayIdentity`] for product showcases (query token or Bearer).

use crafty::{GatewayIdentity, GatewayRequest, IdentityError};

/// Showcase auth: `?user=` (+ optional `?token=` when `GATEWAY_TOKEN` is set) or
/// `Authorization: Bearer` + `X-Crafty-User` / `?user=`.
#[derive(Debug, Clone, Default)]
pub struct ShowcaseGatewayIdentity {
    token_env: String,
}

impl ShowcaseGatewayIdentity {
    /// Read expected token from `GATEWAY_TOKEN`.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new("GATEWAY_TOKEN")
    }

    /// Read expected token from a custom environment variable.
    #[must_use]
    pub fn new(token_env: impl Into<String>) -> Self {
        Self {
            token_env: token_env.into(),
        }
    }

    fn expected_token(&self) -> String {
        std::env::var(&self.token_env).unwrap_or_default()
    }

    fn user_from_parts(req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        if let Some(user) = req.query("user") {
            return Ok(user);
        }
        req.headers
            .get("x-crafty-user")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or(IdentityError::Unauthorized)
    }
}

impl GatewayIdentity for ShowcaseGatewayIdentity {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        let expected = self.expected_token();

        if let Some(bearer) = req.bearer_token() {
            if !expected.is_empty() && bearer != expected {
                return Err(IdentityError::Unauthorized);
            }
            return Self::user_from_parts(req);
        }

        let user = req.query("user").ok_or(IdentityError::Unauthorized)?;
        if expected.is_empty() {
            return Ok(user);
        }
        let token = req.query("token").ok_or(IdentityError::Unauthorized)?;
        if token == expected {
            Ok(user)
        } else {
            Err(IdentityError::Unauthorized)
        }
    }
}
