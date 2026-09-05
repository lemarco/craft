//! Route authentication modes applied before handler dispatch.

/// How a route or subtree is protected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// No auth gate — handler runs immediately after routing.
    #[default]
    Open,
    /// Session cookie gate — placeholder until 0.4.0 wires [`SessionGate`].
    Session,
    /// [`GatewayIdentity`](https://docs.rs/trembita/latest/trembita/gateway/trait.GatewayIdentity.html) extractor — placeholder for built-in API protection.
    Identity,
}

/// Session validation hook — supplied by the product app.
///
/// Full wiring lands in 0.4.0; the type is published now so route tables can declare intent.
#[derive(Debug, Clone)]
pub struct SessionGate {
    /// Cookie name carrying the session token.
    pub cookie_name: String,
}

impl SessionGate {
    /// Declare a session gate for a surface or nested route group.
    #[must_use]
    pub fn new(cookie_name: impl Into<String>) -> Self {
        Self {
            cookie_name: cookie_name.into(),
        }
    }
}
