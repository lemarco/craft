//! App-level feature flags for `trembita new`.

use std::fmt;
use std::str::FromStr;

/// Capabilities enabled in a scaffolded product app.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppFeature {
    /// Job queue + `#[consumer]` handlers.
    Jobs,
    /// HTTP gateway + product APIs.
    Gateway,
    /// OTLP tracing (`trembita-runtime/otlp`).
    Telemetry,
    /// Event topics + subscriptions.
    Topics,
    /// Saga journal + `/workflows/*`.
    Workflows,
    /// Stateful worker actor groups.
    Actors,
    /// Postgres `ExternalBacklog` adapter.
    ExternalBacklog,
    /// Postgres transactional domain outbox.
    DomainOutbox,
}

impl AppFeature {
    /// All supported features.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::Jobs,
            Self::Gateway,
            Self::Telemetry,
            Self::Topics,
            Self::Workflows,
            Self::Actors,
            Self::ExternalBacklog,
            Self::DomainOutbox,
        ]
    }

    /// Default set for `trembita new`.
    #[must_use]
    pub fn defaults() -> Vec<Self> {
        vec![Self::Jobs, Self::Gateway, Self::Telemetry]
    }

    /// Cargo `[features]` key.
    #[must_use]
    pub fn cargo_name(self) -> &'static str {
        match self {
            Self::Jobs => "jobs",
            Self::Gateway => "gateway",
            Self::Telemetry => "telemetry",
            Self::Topics => "topics",
            Self::Workflows => "workflows",
            Self::Actors => "actors",
            Self::ExternalBacklog => "external-backlog",
            Self::DomainOutbox => "domain-outbox",
        }
    }

    /// Parse from CLI (`jobs`, `gateway`, …).
    pub fn parse_name(s: &str) -> Option<Self> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "jobs" | "job" => Self::Jobs,
            "gateway" | "http" => Self::Gateway,
            "telemetry" | "otlp" | "tracing" => Self::Telemetry,
            "topics" | "topic" | "events" => Self::Topics,
            "workflows" | "workflow" | "saga" => Self::Workflows,
            "actors" | "actor" | "workers" => Self::Actors,
            "external-backlog" | "backlog" | "postgres-backlog" => Self::ExternalBacklog,
            "domain-outbox" | "outbox" | "postgres-outbox" => Self::DomainOutbox,
            _ => return None,
        })
    }
}

impl fmt::Display for AppFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.cargo_name())
    }
}

impl FromStr for AppFeature {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_name(s).ok_or_else(|| format!("unknown feature: {s}"))
    }
}

/// Parse a comma-separated feature list; empty → defaults.
pub fn parse_feature_list(raw: &str) -> Result<Vec<AppFeature>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(AppFeature::defaults());
    }
    trimmed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<AppFeature>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_jobs_gateway_telemetry() {
        assert_eq!(
            AppFeature::defaults(),
            vec![AppFeature::Jobs, AppFeature::Gateway, AppFeature::Telemetry]
        );
    }

    #[test]
    fn parses_aliases() {
        assert_eq!(AppFeature::parse_name("otlp"), Some(AppFeature::Telemetry));
        assert_eq!(
            AppFeature::parse_name("postgres-backlog"),
            Some(AppFeature::ExternalBacklog)
        );
    }
}
