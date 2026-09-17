//! Preset scaffolds for `trembita new --template`.

use super::features::AppFeature;

/// Product scenario preset (manifest + default features).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppTemplate {
    /// Durable jobs + sample consumer + HTTP enqueue.
    Jobs,
    /// WebSocket + sticky session casts via capability `chat.append`.
    Realtime,
    /// Saga workflows + default gateway.
    Workflows,
    /// Event topics + publish HTTP.
    Topics,
    /// HTTP gateway + inline capabilities (no default job stream).
    Api,
}

impl AppTemplate {
    /// All template ids for help text.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::Jobs,
            Self::Realtime,
            Self::Workflows,
            Self::Topics,
            Self::Api,
        ]
    }

    /// CLI / docs id.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::Jobs => "jobs",
            Self::Realtime => "realtime",
            Self::Workflows => "workflows",
            Self::Topics => "topics",
            Self::Api => "api",
        }
    }

    /// Default [`AppFeature`] set for this template.
    #[must_use]
    pub fn features(self) -> Vec<AppFeature> {
        match self {
            Self::Jobs => vec![AppFeature::Jobs, AppFeature::Gateway, AppFeature::Telemetry],
            Self::Realtime => vec![AppFeature::Gateway, AppFeature::Telemetry],
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
            Self::Api => vec![AppFeature::Gateway, AppFeature::Telemetry],
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
            "api" | "http" | "gateway" => Self::Api,
            _ => return None,
        })
    }

    /// B-38 alias: `--profile` maps to the same presets as `--template`.
    #[must_use]
    pub fn parse_profile(s: &str) -> Option<Self> {
        Self::parse_name(s)
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

    #[test]
    fn b38_api_profile_omits_jobs_feature() {
        let feats = AppTemplate::Api.features();
        assert!(!feats.contains(&AppFeature::Jobs));
        assert!(feats.contains(&AppFeature::Gateway));
    }

    /// B-38 — `--profile` is an alias for `--template` ([capability-dx § B-38](../../../docs/decisions/capability-dx.md#scale-scaffold-dx-b-38)).
    #[test]
    fn b38_parse_profile_matches_template_aliases_table() {
        struct Row {
            raw: &'static str,
            want: AppTemplate,
        }
        let rows = [
            Row {
                raw: "jobs",
                want: AppTemplate::Jobs,
            },
            Row {
                raw: "job",
                want: AppTemplate::Jobs,
            },
            Row {
                raw: "real-time",
                want: AppTemplate::Realtime,
            },
            Row {
                raw: "ws",
                want: AppTemplate::Realtime,
            },
            Row {
                raw: "api",
                want: AppTemplate::Api,
            },
            Row {
                raw: "gateway",
                want: AppTemplate::Api,
            },
        ];
        for row in rows {
            assert_eq!(
                AppTemplate::parse_profile(row.raw),
                Some(row.want),
                "{}",
                row.raw
            );
            assert_eq!(
                AppTemplate::parse_name(row.raw),
                Some(row.want),
                "{}",
                row.raw
            );
        }
    }

    #[test]
    fn b38_template_id_roundtrip_table() {
        for t in AppTemplate::all() {
            assert_eq!(AppTemplate::parse_profile(t.id()), Some(*t));
        }
    }
}
