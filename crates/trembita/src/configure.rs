//! Runtime / cluster tuning for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::path::PathBuf;
use std::time::Duration;

use trembita_core::Config;

use crate::app::EmptyStateMachine;
use trembita_assembly::TrembitaClusterBuilder;
pub use trembita_assembly::coordination_profile::{
    CoordinationGrowthPreset, CoordinationProfileSpec, parse_coordination_growth_profile,
};

/// Product boot tuning for [`.configure`](super::app::TrembitaAppBuilder::configure).
///
/// Cluster identity and join policy come from [`super::TrembitaAppBuilder::from_env`] /
/// `TREMBITA_*`. [`data_dir`](Self::data_dir) overrides `TREMBITA_DATA_DIR` when set.
///
/// Built-in gateway surfaces (`/health`, `/jobs/*`, …) are **off** by default (`without_* =
/// true`). Opt in by setting the relevant `without_*` field to `false`, or use
/// [`.with_local_gateway_apis`](Self::with_local_gateway_apis) for local dev / tests.
///
/// ```
/// use std::path::PathBuf;
/// use std::time::Duration;
/// use trembita::{TrembitaApp, TrembitaConfigure};
///
/// let _builder = TrembitaApp::builder().configure(
///     TrembitaConfigure::default()
///         .with_data_dir("/tmp/app")
///         .with_local_gateway_apis(),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct TrembitaConfigure {
    /// Persistent storage (`redb`, snapshots, `node-id`). When `None`, uses env / boot merge.
    pub data_dir: Option<PathBuf>,
    /// Raft election / heartbeat tuning.
    pub raft_config: Config,
    /// Wall-clock duration of one logical Raft tick.
    pub tick_period: Duration,
    /// Leader supervisor reconcile interval.
    pub reconcile_period: Duration,
    /// Actor directory publish interval.
    pub directory_publish_period: Duration,
    /// Multi-Raft coordination groups on [`EmptyStateMachine`] (`1` = single group, default).
    pub coordination_raft_groups: u32,
    /// Modulus / stable virtual shard count when [`Self::coordination_raft_groups`] > 1.
    pub coordination_shard_count: Option<u32>,
    /// Named growth profile (B-37) — sets coordination + default queue auto-shard policy.
    pub coordination_growth_preset: Option<CoordinationGrowthPreset>,
    /// B-44 cap on physical auto-shard shards (`TREMBITA_COORDINATION_MAX_QUEUE_SHARDS`).
    pub coordination_max_queue_shards: Option<usize>,
    /// B-44 cap on coordination Raft groups (`TREMBITA_COORDINATION_MAX_RAFT_GROUPS`).
    pub coordination_max_raft_groups: Option<u32>,
    /// Durable cross-node actor mailbox spool (`{data_dir}/mailbox-spool.redb`) — B-41.
    pub durable_mailbox: bool,
    /// When `true`, omit ops HTTP (`/health`, `/ready`, `/metrics`, `/dashboard`, …). Or use
    /// [`TrembitaAppBuilder::without_ops`](crate::TrembitaAppBuilder::without_ops).
    #[cfg(feature = "http-jobs")]
    pub without_ops: bool,
    /// When `true`, omit `POST/GET /jobs/*` on the default gateway. Or use
    /// [`TrembitaAppBuilder::without_jobs_api`](crate::TrembitaAppBuilder::without_jobs_api).
    #[cfg(feature = "http-jobs")]
    pub without_jobs_api: bool,
    /// When `true`, omit job schedule HTTP on the default gateway. Or use
    /// [`TrembitaAppBuilder::without_schedules_api`](crate::TrembitaAppBuilder::without_schedules_api).
    #[cfg(feature = "http-jobs")]
    pub without_schedules_api: bool,
    /// When `true`, omit `/actors/*`. Same as [`TrembitaAppBuilder::without_actors_api`](crate::TrembitaAppBuilder::without_actors_api).
    #[cfg(feature = "http-jobs")]
    pub without_actors_api: bool,
    /// When `true`, omit `POST /workflows/*` on the default gateway. Or use
    /// [`TrembitaAppBuilder::without_workflows_api`](crate::TrembitaAppBuilder::without_workflows_api).
    #[cfg(feature = "http-jobs")]
    pub without_workflows_api: bool,
    /// When `true`, omit topic publish/metrics HTTP on the default gateway. Or use
    /// [`TrembitaAppBuilder::without_topics_api`](crate::TrembitaAppBuilder::without_topics_api).
    #[cfg(feature = "http-jobs")]
    pub without_topics_api: bool,
}

impl Default for TrembitaConfigure {
    fn default() -> Self {
        Self {
            data_dir: None,
            raft_config: Config::default(),
            tick_period: Duration::from_millis(50),
            reconcile_period: Duration::from_millis(250),
            directory_publish_period: Duration::from_millis(250),
            coordination_raft_groups: 1,
            coordination_shard_count: None,
            coordination_growth_preset: None,
            coordination_max_queue_shards: None,
            coordination_max_raft_groups: None,
            durable_mailbox: false,
            #[cfg(feature = "http-jobs")]
            without_ops: true,
            #[cfg(feature = "http-jobs")]
            without_jobs_api: true,
            #[cfg(feature = "http-jobs")]
            without_schedules_api: true,
            #[cfg(feature = "http-jobs")]
            without_actors_api: true,
            #[cfg(feature = "http-jobs")]
            without_workflows_api: true,
            #[cfg(feature = "http-jobs")]
            without_topics_api: true,
        }
    }
}

impl TrembitaConfigure {
    /// Set [`Self::data_dir`].
    #[must_use]
    pub fn with_data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(path.into());
        self
    }

    /// Enable built-in ops + registration-driven product HTTP (not `/actors/*`).
    #[must_use]
    pub fn with_local_gateway_apis(mut self) -> Self {
        #[cfg(feature = "http-jobs")]
        {
            self.without_ops = false;
            self.without_jobs_api = false;
            self.without_schedules_api = false;
            self.without_workflows_api = false;
            self.without_topics_api = false;
        }
        self
    }

    /// Override [`Self::tick_period`].
    #[must_use]
    pub fn with_tick_period(mut self, period: Duration) -> Self {
        self.tick_period = period;
        self
    }

    /// Override [`Self::reconcile_period`].
    #[must_use]
    pub fn with_reconcile_period(mut self, period: Duration) -> Self {
        self.reconcile_period = period;
        self
    }

    /// Override [`Self::directory_publish_period`].
    #[must_use]
    pub fn with_directory_publish_period(mut self, period: Duration) -> Self {
        self.directory_publish_period = period;
        self
    }

    /// Host `count` independent Raft groups for coordination-scale keyed traffic (B-32).
    #[must_use]
    pub fn with_coordination_raft_groups(mut self, count: u32) -> Self {
        self.coordination_raft_groups = count.max(1);
        self
    }

    /// Shard routing table size when multi-Raft coordination is enabled.
    #[must_use]
    pub fn with_coordination_shard_count(mut self, count: u32) -> Self {
        self.coordination_shard_count = Some(count.max(1));
        self
    }

    /// Enable redb mailbox outbox/inbox for cross-node [`/actor/deliver`](../../docs/protocol.md#actor-mailbox-spool-durable-delivery) (requires [`Self::data_dir`] or `TREMBITA_DATA_DIR`).
    #[must_use]
    pub fn with_durable_mailbox(mut self, enabled: bool) -> Self {
        self.durable_mailbox = enabled;
        self
    }

    /// B-44 — cap closed-loop queue shard expansion.
    #[must_use]
    pub fn with_coordination_max_queue_shards(mut self, max: usize) -> Self {
        self.coordination_max_queue_shards = Some(max.max(1));
        self
    }

    /// B-44 — cap runtime [`add_raft_groups`](crate::TrembitaApp::add_raft_groups) growth.
    #[must_use]
    pub fn with_coordination_max_raft_groups(mut self, max: u32) -> Self {
        self.coordination_max_raft_groups = Some(max.max(1));
        self
    }

    /// Apply a B-37 growth profile (coordination + leader auto-shard policy defaults).
    ///
    /// Call **before** [`.manifest`](crate::TrembitaAppBuilder::manifest) / [`.queue`](crate::TrembitaAppBuilder::queue)
    /// so standard queues pick up the profile's auto-shard thresholds. Explicit
    /// [`.with_coordination_raft_groups`](Self::with_coordination_raft_groups) after this method overrides groups.
    #[must_use]
    pub fn with_coordination_growth_preset(mut self, preset: CoordinationGrowthPreset) -> Self {
        let spec = preset.spec();
        self.coordination_growth_preset = Some(preset);
        self.coordination_raft_groups = spec.coordination_raft_groups.max(1);
        self.coordination_shard_count = spec.coordination_shard_count;
        self
    }

    /// Apply Raft / tick settings to a cluster builder.
    #[must_use]
    pub(crate) fn apply_to(
        self,
        inner: TrembitaClusterBuilder<EmptyStateMachine>,
    ) -> TrembitaClusterBuilder<EmptyStateMachine> {
        let mut inner = inner
            .raft_config(self.raft_config)
            .tick_period(self.tick_period)
            .reconcile_period(self.reconcile_period)
            .directory_publish_period(self.directory_publish_period);
        if let Some(dir) = self.data_dir {
            inner = inner.data_dir(dir);
        }
        if self.coordination_raft_groups > 1 {
            let n = usize::try_from(self.coordination_raft_groups).unwrap_or(1);
            inner = inner.raft_machines((0..n).map(|_| EmptyStateMachine));
        }
        if let Some(count) = self.coordination_shard_count {
            inner = inner.shard_count(count);
        }
        if self.durable_mailbox {
            inner = inner.durable_mailbox(true);
        }
        inner.coordination_ceilings(trembita_jobs::CoordinationCeilings {
            max_queue_shards: self.coordination_max_queue_shards,
            max_raft_groups: self.coordination_max_raft_groups,
        })
    }
}

#[cfg(test)]
mod b41_tests {
    use super::*;
    use crate::app::EmptyStateMachine;
    use trembita_assembly::TrembitaClusterBuilder;

    #[test]
    fn b41_durable_mailbox_applies_to_cluster_builder() {
        let cfg = TrembitaConfigure::default()
            .with_data_dir("/tmp/b41-mailbox")
            .with_durable_mailbox(true);
        let inner = TrembitaClusterBuilder::new(trembita_proto::NodeId(1), EmptyStateMachine);
        let _ = cfg.apply_to(inner);
    }

    /// B-41 — [`TrembitaConfigure::with_durable_mailbox`] product flag.
    #[test]
    fn b41_configure_durable_mailbox_scenarios_table() {
        struct Row {
            enabled: bool,
            want: bool,
        }
        let rows = [
            Row {
                enabled: true,
                want: true,
            },
            Row {
                enabled: false,
                want: false,
            },
        ];
        for row in rows {
            let cfg = TrembitaConfigure::default().with_durable_mailbox(row.enabled);
            assert_eq!(cfg.durable_mailbox, row.want, "enabled={}", row.enabled);
        }
        assert!(!TrembitaConfigure::default().durable_mailbox);
    }
}

#[cfg(test)]
mod b32_tests {
    use super::*;
    use trembita_proto::NodeId;

    /// B-32 — [`TrembitaConfigure`] coordination scale knobs (product multi-Raft path).
    #[test]
    fn b32_coordination_configure_scenarios_table() {
        struct Row {
            label: &'static str,
            groups: u32,
            want_groups: u32,
            shard: Option<u32>,
            want_shard: Option<u32>,
        }
        let rows = [
            Row {
                label: "default single group",
                groups: 1,
                want_groups: 1,
                shard: None,
                want_shard: None,
            },
            Row {
                label: "shard count only when groups set in configure",
                groups: 2,
                want_groups: 2,
                shard: Some(32),
                want_shard: Some(32),
            },
            Row {
                label: "explicit multi-Raft",
                groups: 4,
                want_groups: 4,
                shard: Some(128),
                want_shard: Some(128),
            },
            Row {
                label: "zero groups clamps to one",
                groups: 0,
                want_groups: 1,
                shard: Some(0),
                want_shard: Some(1),
            },
        ];
        for row in rows {
            let mut cfg = TrembitaConfigure::default().with_coordination_raft_groups(row.groups);
            if let Some(s) = row.shard {
                cfg = cfg.with_coordination_shard_count(s);
            }
            assert_eq!(
                cfg.coordination_raft_groups, row.want_groups,
                "{}: groups",
                row.label
            );
            if row.shard.is_some() {
                assert_eq!(
                    cfg.coordination_shard_count, row.want_shard,
                    "{}: shard",
                    row.label
                );
            } else {
                assert_eq!(
                    cfg.coordination_shard_count, None,
                    "{}: shard unset",
                    row.label
                );
            }
        }
    }

    #[test]
    fn b32_apply_to_wires_multi_raft_on_empty_state_machine_builder() {
        let inner = TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine);
        let _inner = TrembitaConfigure::default()
            .with_coordination_raft_groups(2)
            .with_coordination_shard_count(64)
            .apply_to(inner);
        // Boot-level assertion lives in `product_coordination_scale` integration tests.
    }
}

#[cfg(test)]
mod b37_tests {
    use super::*;
    use trembita_assembly::coordination_profile::CoordinationGrowthPreset;

    #[test]
    fn b37_configure_growth_preset_scenarios_table() {
        struct Row {
            preset: CoordinationGrowthPreset,
            groups: u32,
            shard: Option<u32>,
        }
        let rows = [
            Row {
                preset: CoordinationGrowthPreset::Standard,
                groups: 1,
                shard: None,
            },
            Row {
                preset: CoordinationGrowthPreset::JobsBacklog,
                groups: 1,
                shard: None,
            },
            Row {
                preset: CoordinationGrowthPreset::WriteSharding,
                groups: 2,
                shard: Some(64),
            },
            Row {
                preset: CoordinationGrowthPreset::Full,
                groups: 2,
                shard: Some(64),
            },
        ];
        for row in rows {
            let cfg = TrembitaConfigure::default().with_coordination_growth_preset(row.preset);
            assert_eq!(cfg.coordination_raft_groups, row.groups, "{:?}", row.preset);
            assert_eq!(cfg.coordination_shard_count, row.shard, "{:?}", row.preset);
            assert_eq!(cfg.coordination_growth_preset, Some(row.preset));
        }
    }

    #[test]
    fn b37_explicit_raft_groups_after_preset_override_profile() {
        let cfg = TrembitaConfigure::default()
            .with_coordination_growth_preset(CoordinationGrowthPreset::WriteSharding)
            .with_coordination_raft_groups(4);
        assert_eq!(cfg.coordination_raft_groups, 4);
        assert_eq!(cfg.coordination_shard_count, Some(64));
    }
}
