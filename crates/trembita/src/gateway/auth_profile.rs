//! Gateway auth composition profiles (B-29) — cookie cluster sessions vs external IdP.

/// How the app composes gateway login (not a built-in OAuth stack).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GatewayAuthProfile {
    /// Login issues cluster-verifiable session cookies; identity at edge via [`super::GatewayIdentity`].
    #[default]
    CookieOnly,
    /// User auth comes from an external IdP hook ([`super::GatewayIdentity`]); optional session cookies afterward.
    ExternalIdp,
}

impl GatewayAuthProfile {
    /// `TREMBITA_GATEWAY_AUTH_PROFILE`: `cookie-only` (default) or `external-idp`.
    #[must_use]
    pub fn from_env() -> Self {
        match std::env::var("TREMBITA_GATEWAY_AUTH_PROFILE")
            .unwrap_or_else(|_| "cookie-only".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "external-idp" | "external_idp" | "oidc" => Self::ExternalIdp,
            _ => Self::CookieOnly,
        }
    }
}
