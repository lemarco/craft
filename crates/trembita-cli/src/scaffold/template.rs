//! Preset scaffolds for `trembita new --template`.

use super::features::AppFeature;

/// Product scenario preset (manifest + default features).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppTemplate {
    /// Durable jobs + sample consumer + HTTP enqueue.
    Jobs,
    /// WebSocket echo + sticky-ready worker group.
    Realtime,
    /// Saga workflows + default gateway.
    Workflows,
    /// Event topics + publish HTTP.
    Topics,
}

impl AppTemplate {
    /// All template ids for help text.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &[Self::Jobs, Self::Realtime, Self::Workflows, Self::Topics]
    }

    /// CLI / docs id.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::Jobs => "jobs",
            Self::Realtime => "realtime",
            Self::Workflows => "workflows",
            Self::Topics => "topics",
        }
    }

    /// Default [`AppFeature`] set for this template.
    #[must_use]
    pub fn features(self) -> Vec<AppFeature> {
        match self {
            Self::Jobs => vec![AppFeature::Jobs, AppFeature::Gateway, AppFeature::Telemetry],
            Self::Realtime => vec![
                AppFeature::Gateway,
                AppFeature::Actors,
                AppFeature::Telemetry,
            ],
            Self::Workflows => vec![
                AppFeature::Workflows,
                AppFeature::Gateway,
                AppFeature::Telemetry,
            ],
            Self::Topics => vec![
                AppFeature::Topics,
                AppFeature::Gateway,
                AppFeature::Telemetry,
            ],
        }
    }

    /// Parse template name from CLI.
    #[must_use]
    pub fn parse_name(s: &str) -> Option<Self> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "jobs" | "job" => Self::Jobs,
            "realtime" | "real-time" | "ws" | "websocket" => Self::Realtime,
            "workflows" | "workflow" | "saga" => Self::Workflows,
            "topics" | "topic" | "events" => Self::Topics,
            _ => return None,
        })
    }
}

/// Resolve features from optional template and explicit `--features` list.
pub fn resolve_scaffold_features(
    template: Option<AppTemplate>,
    features_raw: &str,
) -> Result<Vec<AppFeature>, String> {
    let trimmed = features_raw.trim();
    if !trimmed.is_empty() {
        return super::features::parse_feature_list(trimmed);
    }
    if let Some(t) = template {
        return Ok(t.features());
    }
    Ok(AppFeature::defaults())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_jobs_matches_legacy_default() {
        assert_eq!(AppTemplate::Jobs.features(), AppFeature::defaults());
    }

    #[test]
    fn explicit_features_override_empty_template_default() {
        let feats = resolve_scaffold_features(Some(AppTemplate::Topics), "jobs,gateway").unwrap();
        assert!(feats.contains(&AppFeature::Jobs));
        assert!(!feats.contains(&AppFeature::Topics));
    }
}
