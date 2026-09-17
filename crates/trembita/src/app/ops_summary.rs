//! Ops cockpit snapshot (B-43) — join, scale, R3, coordination preset, queue depths.

use std::sync::Arc;

use serde::Serialize;
use trembita_assembly::coordination_profile::CoordinationGrowthPreset;
use trembita_dashboard::{JoinStatusView, QueueStreamView};
use trembita_jobs::CoordinationClosedLoopSnapshot;

use super::directory_r3::DirectoryR3Snapshot;
use super::runtime::TrembitaApp;
use super::scale_plan::ProductScalePlan;

/// Per-stream depth hints (subset of [`QueueStreamView`](trembita_dashboard::QueueStreamView)).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpsQueueDepthHint {
    /// Job stream name.
    pub stream: String,
    /// Ready jobs waiting for lease.
    pub pending: u64,
    /// Currently leased jobs.
    pub leased: u64,
    /// Age of oldest ready pending job in milliseconds.
    pub oldest_pending_age_ms: u64,
}

impl From<&QueueStreamView> for OpsQueueDepthHint {
    fn from(s: &QueueStreamView) -> Self {
        Self {
            stream: s.stream.clone(),
            pending: s.pending,
            leased: s.leased,
            oldest_pending_age_ms: s.oldest_pending_age_ms,
        }
    }
}

/// B-37 preset recorded at boot (resolved layout lives under [`OpsSummary::product_scale`]).
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct CoordinationProfileSnapshot {
    /// `TREMBITA_COORDINATION_PROFILE` / [`.with_coordination_growth_preset`](crate::TrembitaConfigure::with_coordination_growth_preset).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preset: Option<String>,
    /// Whether env-only job queue auto-shard is enabled for this preset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_job_queue_auto_shard: Option<bool>,
    /// Leader auto-shard backlog threshold when preset enables growth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_shard_pending_threshold: Option<u64>,
    /// Leader auto-shard physical shard ceiling when preset enables growth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_shard_max_shards: Option<usize>,
}

impl CoordinationProfileSnapshot {
    #[must_use]
    fn from_preset(preset: CoordinationGrowthPreset) -> Self {
        let spec = preset.spec();
        let mut snap = Self {
            preset: Some(preset.env_key().into()),
            env_job_queue_auto_shard: Some(spec.env_job_queue_auto_shard),
            auto_shard_pending_threshold: None,
            auto_shard_max_shards: None,
        };
        if spec.env_job_queue_auto_shard {
            snap.auto_shard_pending_threshold = Some(spec.auto_shard_policy.pending_threshold);
            snap.auto_shard_max_shards = Some(spec.auto_shard_policy.max_shards);
        }
        snap
    }
}

/// Single JSON snapshot for operators — **`GET /introspect/ops-summary`** on the unified listener.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpsSummary {
    /// Elastic join pipeline ([B-35](../../../docs/decisions/cluster-elasticity.md)).
    pub join: JoinStatusView,
    /// Capability hosts, queue registration layout, coordination Raft counts (B-33 product scale plan).
    pub product_scale: ProductScalePlan,
    /// R3 directory visibility (B-36 merge lag / delivery snapshot).
    pub directory_r3: DirectoryR3Snapshot,
    /// Named growth preset when configured at boot ([B-37](../../../docs/decisions/capability-dx.md)).
    pub coordination_profile: CoordinationProfileSnapshot,
    /// Live queue depth hints from the observer (same source as `/introspect/queues`).
    pub queue_depths: Vec<OpsQueueDepthHint>,
    /// B-44 auto-shard / Raft ceiling state and `why_not_scaling` hints.
    pub coordination_closed_loop: CoordinationClosedLoopSnapshot,
}

impl TrembitaApp {
    /// Point-in-time ops cockpit JSON (async: reads live readiness + queue depths).
    pub async fn ops_summary(&self) -> OpsSummary {
        let observer = self.introspect_observer();
        let readiness = observer.readiness().await;
        let queues = observer.queues().await;
        OpsSummary {
            join: JoinStatusView::from_readiness(&readiness),
            product_scale: self.scale_plan().clone(),
            directory_r3: self.directory_r3_snapshot(),
            coordination_profile: self
                .coordination_growth_preset()
                .map(CoordinationProfileSnapshot::from_preset)
                .unwrap_or_default(),
            queue_depths: queues.streams.iter().map(OpsQueueDepthHint::from).collect(),
            coordination_closed_loop: self.coordination_closed_loop_snapshot(),
        }
    }

    /// B-44 closed-loop growth snapshot (leader auto-shard registry + Raft hints).
    #[must_use]
    pub fn coordination_closed_loop_snapshot(&self) -> CoordinationClosedLoopSnapshot {
        self.cluster().coordination_closed_loop_snapshot()
    }
}

#[cfg(feature = "http-jobs")]
pub(crate) fn ops_summary_route_table(app: Arc<TrembitaApp>) -> trembita_http::RouteTable {
    use http::StatusCode;
    use trembita_http::{RequestCtx, Response, RouteTable};

    RouteTable::new().get("/introspect/ops-summary", move |_ctx: RequestCtx| {
        let app = Arc::clone(&app);
        async move {
            let summary = app.ops_summary().await;
            Ok(Response::json(
                StatusCode::OK,
                serde_json::to_value(summary).unwrap_or_default(),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use trembita_assembly::coordination_profile::CoordinationGrowthPreset;
    use trembita_dashboard::{JoinPhase, QueueStreamView};

    #[test]
    fn b43_queue_depth_hint_from_stream_view() {
        let view = QueueStreamView {
            stream: "app.jobs".into(),
            pending: 12,
            leased: 3,
            dead_letter: 0,
            oldest_pending_age_ms: 900,
            redelivered: 1,
        };
        let hint = OpsQueueDepthHint::from(&view);
        assert_eq!(hint.stream, "app.jobs");
        assert_eq!(hint.pending, 12);
        assert_eq!(hint.leased, 3);
        assert_eq!(hint.oldest_pending_age_ms, 900);
    }

    /// B-43 — preset → introspect profile lines (mirrors [capabilities § B-37](../../../docs/scenarios/capabilities.md#coordination-growth-presets-b-37)).
    #[test]
    fn b43_coordination_profile_presets_scenarios_table() {
        struct Row {
            preset: CoordinationGrowthPreset,
            env_key: &'static str,
            env_auto: bool,
            auto_shard_hints: bool,
        }
        let rows = [
            Row {
                preset: CoordinationGrowthPreset::Standard,
                env_key: "standard",
                env_auto: false,
                auto_shard_hints: false,
            },
            Row {
                preset: CoordinationGrowthPreset::JobsBacklog,
                env_key: "jobs_backlog",
                env_auto: true,
                auto_shard_hints: true,
            },
            Row {
                preset: CoordinationGrowthPreset::WriteSharding,
                env_key: "write_sharding",
                env_auto: false,
                auto_shard_hints: false,
            },
            Row {
                preset: CoordinationGrowthPreset::Full,
                env_key: "full",
                env_auto: true,
                auto_shard_hints: true,
            },
        ];
        for row in rows {
            let snap = CoordinationProfileSnapshot::from_preset(row.preset);
            assert_eq!(snap.preset.as_deref(), Some(row.env_key), "preset key");
            assert_eq!(
                snap.env_job_queue_auto_shard,
                Some(row.env_auto),
                "{}: env_job_queue_auto_shard",
                row.env_key
            );
            assert_eq!(
                snap.auto_shard_pending_threshold.is_some(),
                row.auto_shard_hints,
                "{}: pending threshold",
                row.env_key
            );
            assert_eq!(
                snap.auto_shard_max_shards.is_some(),
                row.auto_shard_hints,
                "{}: max shards",
                row.env_key
            );
        }
    }

    #[test]
    fn b43_coordination_profile_default_serializes_empty_object() {
        let json = serde_json::to_value(CoordinationProfileSnapshot::default()).expect("json");
        assert_eq!(json, serde_json::json!({}));
    }

    #[test]
    fn b43_queue_depth_hint_scenarios_table() {
        struct Row {
            stream: &'static str,
            pending: u64,
            leased: u64,
            age_ms: u64,
        }
        let rows = [
            Row {
                stream: "idle",
                pending: 0,
                leased: 0,
                age_ms: 0,
            },
            Row {
                stream: "hot",
                pending: 4096,
                leased: 12,
                age_ms: 120_000,
            },
        ];
        for row in rows {
            let view = QueueStreamView {
                stream: row.stream.into(),
                pending: row.pending,
                leased: row.leased,
                dead_letter: 99,
                oldest_pending_age_ms: row.age_ms,
                redelivered: 7,
            };
            let hint = OpsQueueDepthHint::from(&view);
            assert_eq!(hint.stream, row.stream);
            assert_eq!(hint.pending, row.pending);
            assert_eq!(hint.leased, row.leased);
            assert_eq!(hint.oldest_pending_age_ms, row.age_ms);
        }
    }

    #[test]
    fn b43_join_phase_json_scenarios_table() {
        let phases = [
            (JoinPhase::AwaitingMembership, "awaiting_membership"),
            (JoinPhase::CatchingUp, "catching_up"),
            (JoinPhase::AwaitingHosts, "awaiting_hosts"),
            (JoinPhase::PoolReady, "pool_ready"),
        ];
        for (phase, want) in phases {
            let summary = OpsSummary {
                join: JoinStatusView {
                    node_id: 2,
                    phase,
                    committed_voter: false,
                    committed_learner: true,
                    log_caught_up: phase != JoinPhase::AwaitingMembership,
                    hosts_wired: phase == JoinPhase::PoolReady,
                    local_workers: vec!["w#1".into()],
                    reason: Some("scenario".into()),
                },
                product_scale: ProductScalePlan::default(),
                directory_r3: DirectoryR3Snapshot {
                    directory_policy: "read_your_writes".into(),
                    directory_retry_max_attempts: 8,
                    directory_retry_backoff_ms: 25,
                    directory_retry_boost_active: false,
                    local_directory_epoch: 0,
                    merge_lag_epochs: 0,
                    deliver_no_target_totals: Default::default(),
                },
                coordination_profile: CoordinationProfileSnapshot::default(),
                queue_depths: Vec::new(),
                coordination_closed_loop: CoordinationClosedLoopSnapshot::default(),
            };
            let json = serde_json::to_value(&summary).expect("serialize");
            assert_eq!(json["join"]["phase"], want, "join phase json");
        }
    }

    #[test]
    fn b43_ops_summary_json_includes_join_and_scale_sections() {
        let summary = OpsSummary {
            join: JoinStatusView {
                node_id: 1,
                phase: JoinPhase::PoolReady,
                committed_voter: true,
                committed_learner: false,
                log_caught_up: true,
                hosts_wired: true,
                local_workers: vec!["cap#1".into()],
                reason: None,
            },
            product_scale: ProductScalePlan::default(),
            directory_r3: DirectoryR3Snapshot {
                directory_policy: "read_your_writes".into(),
                directory_retry_max_attempts: 8,
                directory_retry_backoff_ms: 25,
                directory_retry_boost_active: false,
                local_directory_epoch: 1,
                merge_lag_epochs: 0,
                deliver_no_target_totals: Default::default(),
            },
            coordination_profile: CoordinationProfileSnapshot::default(),
            queue_depths: Vec::new(),
            coordination_closed_loop: CoordinationClosedLoopSnapshot::default(),
        };
        let json = serde_json::to_value(&summary).expect("serialize");
        assert_eq!(json["join"]["phase"], "pool_ready");
        assert!(json.get("product_scale").is_some());
        assert!(json.get("directory_r3").is_some());
        assert!(json.get("queue_depths").is_some());
        assert!(json.get("coordination_closed_loop").is_some());
    }
}
