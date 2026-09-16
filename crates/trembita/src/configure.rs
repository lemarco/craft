//! Runtime / cluster tuning for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::path::PathBuf;
use std::time::Duration;

use trembita_core::Config;

use crate::app::EmptyStateMachine;
use trembita_assembly::TrembitaClusterBuilder;

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
    /// When `true`, omit ops HTTP (`/health`, `/ready`, `/metrics`, `/dashboard`, …).
    #[cfg(feature = "http-jobs")]
    pub without_ops: bool,
    /// When `true`, omit `POST/GET /jobs/*` on the default gateway.
    #[cfg(feature = "http-jobs")]
    pub without_jobs_api: bool,
    /// When `true`, omit job schedule HTTP on the default gateway.
    #[cfg(feature = "http-jobs")]
    pub without_schedules_api: bool,
    /// When `true`, omit `/actors/*` (use [`WorkerOpts::http_cast`](crate::WorkerOpts::http_cast)).
    #[cfg(feature = "http-jobs")]
    pub without_actors_api: bool,
    /// When `true`, omit `POST /workflows/*` on the default gateway.
    #[cfg(feature = "http-jobs")]
    pub without_workflows_api: bool,
    /// When `true`, omit topic publish/metrics HTTP on the default gateway.
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
        inner
    }
}
