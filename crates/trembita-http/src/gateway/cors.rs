//! CORS policy for a gateway surface (0.4.0).

use std::env;
use std::time::Duration;

/// Cross-origin policy attached to a [`super::Surface`](super::Surface).
#[derive(Debug, Clone)]
pub struct CorsPolicy {
    /// Allowed `Origin` values (exact or `*.domain` wildcards).
    pub origins: Vec<String>,
    /// Whether `Access-Control-Allow-Credentials` is set.
    pub credentials: bool,
    /// Preflight cache duration.
    pub max_age: Duration,
}

impl CorsPolicy {
    /// Build a CORS policy with credentials enabled (required for cookie sessions).
    #[must_use]
    pub fn credentials(origins: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            origins: origins.into_iter().map(Into::into).collect(),
            credentials: true,
            max_age: Duration::from_secs(3600),
        }
    }

    /// Read `{prefix}_CORS_ORIGINS` (comma-separated) and `{prefix}_CORS_CREDENTIALS` (`true`/`false`).
    #[must_use]
    pub fn from_env(prefix: &str) -> Option<Self> {
        let raw = env::var(format!("{prefix}_CORS_ORIGINS")).ok()?;
        let origins: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if origins.is_empty() {
            return None;
        }
        let credentials = env::var(format!("{prefix}_CORS_CREDENTIALS"))
            .map(|v| is_true(&v))
            .unwrap_or(true);
        Some(Self {
            origins,
            credentials,
            max_age: Duration::from_secs(3600),
        })
    }

    /// Whether `origin` is allowed.
    #[must_use]
    pub fn allows_origin(&self, origin: &str) -> bool {
        self.origins
            .iter()
            .any(|pattern| origin_matches(pattern, origin))
    }
}

fn origin_matches(pattern: &str, origin: &str) -> bool {
    if pattern == origin {
        return true;
    }
    let Some(suffix) = pattern.strip_prefix("*.") else {
        return false;
    };
    let host = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
        .unwrap_or(origin);
    host == suffix || host.ends_with(&format!(".{suffix}"))
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
    fn exact_origin_match() {
        let cors = CorsPolicy::credentials(["https://app.example.com"]);
        assert!(cors.allows_origin("https://app.example.com"));
        assert!(!cors.allows_origin("https://evil.example.com"));
    }

    #[test]
    fn wildcard_subdomain_match() {
        let cors = CorsPolicy::credentials(["*.example.com"]);
        assert!(cors.allows_origin("https://app.example.com"));
        assert!(cors.allows_origin("http://api.example.com"));
        assert!(!cors.allows_origin("https://example.com.evil.net"));
    }
}
