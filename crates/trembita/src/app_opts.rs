//! Run options for [`TrembitaApp`](super::app::TrembitaApp).

use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;
use trembita_net::LocalNetwork;

use crate::AppManifest;
use crate::ReadyOpts;
use crate::app::ManifestRunHint;
use crate::app::{ShutdownOpts, TrembitaApp};
use trembita_assembly::env_config::AppConfig;

/// Custom shutdown future for [`RunOpts::with_shutdown_signal`].
pub type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Boot + block until shutdown signal + graceful shutdown for [`TrembitaAppBuilder::run`](super::app::TrembitaAppBuilder::run).
///
/// Every product node runs as a QUIC cluster member (seed or joiner) configured via `TREMBITA_*` env.
pub struct RunOpts {
    /// In-memory transport for integration tests only ([`RunOpts::local`]).
    pub(crate) local_net: Option<LocalNetwork>,
    /// Poll for leader / optional queue mount after boot.
    pub wait_ready: Option<ReadyOpts>,
    /// Graceful teardown after the signal.
    pub shutdown: ShutdownOpts,
    /// Optional custom shutdown future (defaults to SIGINT + SIGTERM on Unix).
    pub(crate) shutdown_signal: Option<ShutdownSignal>,
}

impl Default for RunOpts {
    fn default() -> Self {
        Self {
            local_net: None,
            wait_ready: None,
            shutdown: ShutdownOpts::from_env(),
            shutdown_signal: None,
        }
    }
}

impl RunOpts {
    /// Boot/shutdown options from a parsed [`AppConfig`].
    #[must_use]
    pub fn from_config(cfg: &AppConfig) -> Self {
        let mut opts = Self {
            shutdown: ShutdownOpts {
                graceful_leave: cfg.graceful_leave,
                ..ShutdownOpts::default()
            },
            ..Self::default()
        };
        if let Some(stream) = cfg.job_queue_stream.as_ref().filter(|s| !s.is_empty()) {
            opts = opts.with_wait_queue(stream);
        }
        opts
    }

    /// Boot/shutdown options from `TREMBITA_*` (`TREMBITA_GRACEFUL_LEAVE`, `TREMBITA_JOB_QUEUE` → wait-for-queue).
    ///
    /// # Errors
    /// Invalid environment (same as [`trembita_assembly::env_config::app_config_from_env`]).
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::from_config(
            &trembita_assembly::env_config::app_config_from_env()?,
        ))
    }

    /// [`Self::from_config`] plus readiness from [`AppManifest`]: one job stream → wait-for-queue; several → use env or [`.with_wait_queue`](Self::with_wait_queue).
    #[must_use]
    pub fn for_manifest(cfg: &AppConfig, manifest: &AppManifest) -> Self {
        Self::from_config(cfg).with_manifest(manifest)
    }

    /// When [`Self::wait_ready`] is unset: derive readiness from manifest jobs / workers.
    #[must_use]
    pub fn with_manifest(self, manifest: &AppManifest) -> Self {
        let mut hint = ManifestRunHint::default();
        hint.apply_manifest(manifest.job_stream_names(), manifest.has_workers());
        self.with_run_hint(&hint)
    }

    /// Apply registration-derived readiness (from builder or manifest).
    #[must_use]
    pub(crate) fn with_run_hint(self, hint: &ManifestRunHint) -> Self {
        if self.wait_ready.is_some() {
            return self;
        }
        let needs_ready = hint.has_workers || !hint.job_streams.is_empty();
        if !needs_ready {
            return self;
        }
        let mut ready = ReadyOpts::default();
        if let [stream] = hint.job_streams.as_slice() {
            ready = ready.with_queue(stream);
        }
        self.with_wait_ready(ready)
    }

    /// Poll until the cluster (and optional queue) is ready after boot.
    #[must_use]
    pub fn with_wait_ready(mut self, opts: ReadyOpts) -> Self {
        self.wait_ready = Some(opts);
        self
    }

    /// Poll until `stream` is mounted on the queue leader.
    #[must_use]
    pub fn with_wait_queue(mut self, stream: &str) -> Self {
        self.wait_ready = Some(ReadyOpts::default().with_queue(stream));
        self
    }

    /// Wire job queue consumer stop handles into graceful shutdown.
    #[must_use]
    pub fn with_consumers(
        mut self,
        stop: tokio::sync::watch::Sender<bool>,
        handles: Vec<JoinHandle<()>>,
    ) -> Self {
        self.shutdown.consumers = Some((stop, handles));
        self
    }

    /// Integration tests only — in-memory [`LocalNetwork`], not for product binaries.
    #[doc(hidden)]
    #[must_use]
    pub fn local() -> Self {
        Self {
            local_net: Some(LocalNetwork::new()),
            ..Self::default()
        }
    }

    /// Integration tests only — boot on an existing in-memory network (multi-node local cluster).
    #[doc(hidden)]
    #[must_use]
    pub fn with_local_net(mut self, net: LocalNetwork) -> Self {
        self.local_net = Some(net);
        self
    }

    /// Replace the default SIGINT/SIGTERM wait with a custom future (tests, embedders).
    #[must_use]
    pub fn with_shutdown_signal(
        mut self,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        self.shutdown_signal = Some(Box::pin(signal));
        self
    }
}

impl ShutdownOpts {
    /// Read `TREMBITA_GRACEFUL_LEAVE` (defaults to `true` in product mode).
    #[must_use]
    pub fn from_env() -> Self {
        TrembitaApp::shutdown_opts_from_env()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RunOpts;
    use crate::AppManifest;
    use crate::JobOpts;

    #[test]
    fn with_manifest_waits_on_single_job_stream() {
        let manifest =
            AppManifest::new().jobs([JobOpts::new("emails").lease(Duration::from_secs(60))]);
        let opts = RunOpts::default().with_manifest(&manifest);
        assert!(opts.wait_ready.is_some());
    }

    #[test]
    fn with_manifest_leader_wait_when_multiple_job_streams() {
        let manifest = AppManifest::new().jobs([JobOpts::new("a"), JobOpts::new("b")]);
        let opts = RunOpts::default().with_manifest(&manifest);
        let ready = opts.wait_ready.expect("leader wait");
        assert!(ready.job_streams.is_empty());
    }

    #[test]
    fn with_run_hint_waits_for_leader_when_workers_only() {
        use crate::app::ManifestRunHint;

        let hint = ManifestRunHint {
            has_workers: true,
            ..ManifestRunHint::default()
        };
        let opts = RunOpts::default().with_run_hint(&hint);
        assert!(opts.wait_ready.is_some());
    }
}
