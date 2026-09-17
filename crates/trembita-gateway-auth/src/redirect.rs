//! OAuth redirect URI allowlist (B-47).

use std::collections::BTreeSet;

/// Exact-match allowlist for `redirect_uri` / post-login redirects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedirectAllowlist {
    entries: BTreeSet<String>,
}

impl RedirectAllowlist {
    /// Empty list (deny all until entries added).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            entries: BTreeSet::new(),
        }
    }

    /// Parse comma- or whitespace-separated absolute URIs (no globbing).
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        let mut entries = BTreeSet::new();
        for part in raw.split([',', '\n', ' ']) {
            let uri = part.trim();
            if !uri.is_empty() {
                entries.insert(uri.to_string());
            }
        }
        Self { entries }
    }

    /// Load from env (`TREMBITA_OAUTH_REDIRECT_ALLOWLIST` or custom name). `None` when unset/empty.
    #[must_use]
    pub fn from_env(var: &str) -> Option<Self> {
        std::env::var(var)
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| Self::parse(&s))
            .filter(|l| !l.entries.is_empty())
    }

    /// Register one redirect URI (exact string match at runtime).
    pub fn allow(&mut self, redirect_uri: impl Into<String>) {
        let u = redirect_uri.into();
        if !u.is_empty() {
            self.entries.insert(u);
        }
    }

    /// Whether `redirect_uri` is explicitly allowed.
    #[must_use]
    pub fn allows(&self, redirect_uri: &str) -> bool {
        self.entries.contains(redirect_uri)
    }

    /// Entries for introspect / tests.
    pub fn entries(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(String::as_str)
    }
}

/// Redirect validation failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedirectError {
    /// URI not on the allowlist.
    #[error("redirect_uri not allowed")]
    NotAllowed,
    /// Missing required parameter.
    #[error("missing redirect_uri")]
    Missing,
}

impl RedirectAllowlist {
    /// # Errors
    /// [`RedirectError`] when not allowlisted.
    pub fn check(&self, redirect_uri: Option<&str>) -> Result<(), RedirectError> {
        let Some(uri) = redirect_uri.filter(|s| !s.is_empty()) else {
            return Err(RedirectError::Missing);
        };
        if self.allows(uri) {
            Ok(())
        } else {
            Err(RedirectError::NotAllowed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b47_redirect_allowlist_exact_match_table() {
        let list = RedirectAllowlist::parse(
            "https://app.example/oauth/callback, https://app.example/mobile/cb",
        );
        assert!(list.allows("https://app.example/oauth/callback"));
        assert!(!list.allows("https://app.example/oauth/callback/"));
        assert!(!list.allows("https://evil.example/oauth/callback"));
    }

    #[test]
    fn b47_redirect_check_missing_and_denied() {
        let list = RedirectAllowlist::parse("https://ok/cb");
        assert_eq!(list.check(None), Err(RedirectError::Missing));
        assert_eq!(
            list.check(Some("https://no/cb")),
            Err(RedirectError::NotAllowed)
        );
        assert!(list.check(Some("https://ok/cb")).is_ok());
    }
}
