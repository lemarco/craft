//! CORS policy for a gateway surface (0.4.0).

use std::time::Duration;

/// Cross-origin policy attached to a [`super::Surface`](super::Surface).
#[derive(Debug, Clone)]
pub struct CorsPolicy {
    /// Allowed `Origin` values.
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

    /// Whether `origin` is allowed.
    #[must_use]
    pub fn allows_origin(&self, origin: &str) -> bool {
        self.origins.iter().any(|o| o == origin)
    }
}
