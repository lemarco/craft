//! Session cookie configuration from environment variables.

use std::env;

/// HTTP session cookie attributes for one product surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CookieConfig {
    /// Cookie name carrying the session token.
    pub name: String,
    /// Lifetime in seconds.
    pub max_age: i64,
    /// `Secure` attribute.
    pub secure: bool,
    /// `HttpOnly` attribute.
    pub http_only: bool,
    /// `SameSite` attribute — `strict`, `lax`, or `none`.
    pub same_site: String,
}

impl CookieConfig {
    /// Read `{prefix}_COOKIE_*` variables with defaults suitable for local dev.
    ///
    /// | Variable | Default |
    /// |----------|---------|
    /// | `{prefix}_COOKIE_NAME` | `default_name` |
    /// | `{prefix}_COOKIE_MAX_AGE` | `604800` |
    /// | `{prefix}_COOKIE_SECURE` | `false` |
    /// | `{prefix}_COOKIE_HTTP_ONLY` | `true` |
    /// | `{prefix}_COOKIE_SAME_SITE` | `lax` |
    #[must_use]
    pub fn from_env(prefix: &str, default_name: &str) -> Self {
        Self {
            name: env_var(&format!("{prefix}_COOKIE_NAME"), default_name),
            max_age: env_var(&format!("{prefix}_COOKIE_MAX_AGE"), "604800")
                .parse()
                .unwrap_or(604_800),
            secure: is_true(&env_var(&format!("{prefix}_COOKIE_SECURE"), "false")),
            http_only: is_true(&env_var(&format!("{prefix}_COOKIE_HTTP_ONLY"), "true")),
            same_site: env_var(&format!("{prefix}_COOKIE_SAME_SITE"), "lax"),
        }
    }

    /// Build the `Set-Cookie` header value for a session token.
    #[must_use]
    pub fn set_cookie_value(&self, token: &str) -> String {
        let same_site = self.same_site.to_ascii_lowercase();
        let mut value = format!(
            "{}={token}; Max-Age={}; Path=/; SameSite={same_site}",
            self.name, self.max_age
        );
        if self.http_only {
            value.push_str("; HttpOnly");
        }
        if self.secure {
            value.push_str("; Secure");
        }
        value
    }
}

fn env_var(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn is_true(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_defaults_without_env() {
        let cfg = CookieConfig::from_env("TEST_CLIENT", "session");
        assert_eq!(cfg.name, "session");
        assert_eq!(cfg.max_age, 604_800);
        assert!(!cfg.secure);
        assert!(cfg.http_only);
        assert_eq!(cfg.same_site, "lax");
    }
}
