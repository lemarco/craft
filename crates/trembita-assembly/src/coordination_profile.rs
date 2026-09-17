//! Product coordination growth presets (B-37) — env + [`TrembitaConfigure`](../../crates/trembita/src/configure.rs).

use trembita_jobs::AutoShardPolicy;

/// Named profile for B-32 coordination knobs (queue auto-shard + multi-Raft).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinationGrowthPreset {
    /// Single Raft group, standard queue layout (default).
    Standard,
    /// Leader expands queue physical shards under sustained backlog depth.
    JobsBacklog,
    /// Multi-Raft coordination SM for keyed write ceiling.
    WriteSharding,
    /// Auto-shard queues + multi-Raft (large deployments).
    Full,
}

/// Resolved knobs for one [`CoordinationGrowthPreset`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinationProfileSpec {
    /// Multi-Raft coordination groups (`1` = default).
    pub coordination_raft_groups: u32,
    /// Shard routing table when groups > 1.
    pub coordination_shard_count: Option<u32>,
    /// Enable adaptive shards for env-only `TREMBITA_JOB_QUEUE`.
    pub env_job_queue_auto_shard: bool,
    /// Leader auto-shard thresholds (depth / ceiling).
    pub auto_shard_policy: AutoShardPolicy,
}

impl CoordinationGrowthPreset {
    /// Wire name for `TREMBITA_COORDINATION_PROFILE`.
    #[must_use]
    pub fn env_key(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::JobsBacklog => "jobs_backlog",
            Self::WriteSharding => "write_sharding",
            Self::Full => "full",
        }
    }

    /// Resolved coordination + queue growth knobs.
    #[must_use]
    pub fn spec(self) -> CoordinationProfileSpec {
        match self {
            Self::Standard => CoordinationProfileSpec {
                coordination_raft_groups: 1,
                coordination_shard_count: None,
                env_job_queue_auto_shard: false,
                auto_shard_policy: AutoShardPolicy::default(),
            },
            Self::JobsBacklog => CoordinationProfileSpec {
                coordination_raft_groups: 1,
                coordination_shard_count: None,
                env_job_queue_auto_shard: true,
                auto_shard_policy: AutoShardPolicy::jobs_backlog_growth(),
            },
            Self::WriteSharding => CoordinationProfileSpec {
                coordination_raft_groups: 2,
                coordination_shard_count: Some(64),
                env_job_queue_auto_shard: false,
                auto_shard_policy: AutoShardPolicy::default(),
            },
            Self::Full => CoordinationProfileSpec {
                coordination_raft_groups: 2,
                coordination_shard_count: Some(64),
                env_job_queue_auto_shard: true,
                auto_shard_policy: AutoShardPolicy::full_growth(),
            },
        }
    }
}

/// Parse `TREMBITA_COORDINATION_PROFILE` (`standard`, `jobs_backlog`, `write_sharding`, `full`).
#[must_use]
pub fn parse_coordination_growth_profile(raw: &str) -> Option<CoordinationGrowthPreset> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "standard" | "" => Some(CoordinationGrowthPreset::Standard),
        "jobs_backlog" | "jobs-backlog" | "jobs" => Some(CoordinationGrowthPreset::JobsBacklog),
        "write_sharding" | "write-sharding" | "sharding" | "write" => {
            Some(CoordinationGrowthPreset::WriteSharding)
        }
        "full" | "growth" => Some(CoordinationGrowthPreset::Full),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b37_profile_parse_table() {
        assert_eq!(
            parse_coordination_growth_profile("jobs_backlog"),
            Some(CoordinationGrowthPreset::JobsBacklog)
        );
        assert_eq!(
            parse_coordination_growth_profile("FULL"),
            Some(CoordinationGrowthPreset::Full)
        );
        assert!(parse_coordination_growth_profile("nope").is_none());
    }

    #[test]
    fn b37_write_sharding_spec_sets_multi_raft() {
        let spec = CoordinationGrowthPreset::WriteSharding.spec();
        assert_eq!(spec.coordination_raft_groups, 2);
        assert_eq!(spec.coordination_shard_count, Some(64));
        assert!(!spec.env_job_queue_auto_shard);
    }

    #[test]
    fn b37_jobs_backlog_auto_shard_policy_has_ceiling() {
        let spec = CoordinationGrowthPreset::JobsBacklog.spec();
        assert!(spec.env_job_queue_auto_shard);
        assert!(spec.auto_shard_policy.max_shards >= 8);
        assert!(spec.auto_shard_policy.pending_threshold > 0);
    }

    /// B-37 — all presets resolve to the capability map in [capabilities § B-37](../../../docs/scenarios/capabilities.md#coordination-growth-presets-b-37).
    #[test]
    fn b37_profile_spec_scenarios_table() {
        use trembita_jobs::AutoShardPolicy;

        struct Row {
            preset: CoordinationGrowthPreset,
            groups: u32,
            shard: Option<u32>,
            env_auto: bool,
            policy: fn() -> AutoShardPolicy,
        }
        let rows = [
            Row {
                preset: CoordinationGrowthPreset::Standard,
                groups: 1,
                shard: None,
                env_auto: false,
                policy: AutoShardPolicy::default,
            },
            Row {
                preset: CoordinationGrowthPreset::JobsBacklog,
                groups: 1,
                shard: None,
                env_auto: true,
                policy: AutoShardPolicy::jobs_backlog_growth,
            },
            Row {
                preset: CoordinationGrowthPreset::WriteSharding,
                groups: 2,
                shard: Some(64),
                env_auto: false,
                policy: AutoShardPolicy::default,
            },
            Row {
                preset: CoordinationGrowthPreset::Full,
                groups: 2,
                shard: Some(64),
                env_auto: true,
                policy: AutoShardPolicy::full_growth,
            },
        ];
        for row in rows {
            let spec = row.preset.spec();
            assert_eq!(
                spec.coordination_raft_groups, row.groups,
                "{:?}",
                row.preset
            );
            assert_eq!(spec.coordination_shard_count, row.shard, "{:?}", row.preset);
            assert_eq!(
                spec.env_job_queue_auto_shard, row.env_auto,
                "{:?}",
                row.preset
            );
            assert_eq!(spec.auto_shard_policy, (row.policy)(), "{:?}", row.preset);
        }
    }

    #[test]
    fn b37_profile_env_key_matches_parse_roundtrip() {
        for preset in [
            CoordinationGrowthPreset::Standard,
            CoordinationGrowthPreset::JobsBacklog,
            CoordinationGrowthPreset::WriteSharding,
            CoordinationGrowthPreset::Full,
        ] {
            let key = preset.env_key();
            assert_eq!(
                parse_coordination_growth_profile(key),
                Some(preset),
                "key={key}"
            );
        }
    }

    #[test]
    fn b37_profile_parse_aliases_table() {
        struct Row {
            raw: &'static str,
            want: Option<CoordinationGrowthPreset>,
        }
        let rows = [
            Row {
                raw: "",
                want: Some(CoordinationGrowthPreset::Standard),
            },
            Row {
                raw: "jobs-backlog",
                want: Some(CoordinationGrowthPreset::JobsBacklog),
            },
            Row {
                raw: "write-sharding",
                want: Some(CoordinationGrowthPreset::WriteSharding),
            },
            Row {
                raw: "growth",
                want: Some(CoordinationGrowthPreset::Full),
            },
            Row {
                raw: "unknown",
                want: None,
            },
        ];
        for row in rows {
            assert_eq!(
                parse_coordination_growth_profile(row.raw),
                row.want,
                "raw={}",
                row.raw
            );
        }
    }
}
