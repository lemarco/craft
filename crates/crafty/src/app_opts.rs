//! Run options for [`CraftyApp`](super::app::CraftyApp).

use crafty_net::LocalNetwork;
use tokio::task::JoinHandle;

use crate::ReadyOpts;
use crate::app::{CraftyApp, ShutdownOpts};

/// Boot + block until Ctrl-C + graceful shutdown for [`CraftyAppBuilder::run`](super::app::CraftyAppBuilder::run).
///
/// Every product node runs as a QUIC cluster member (seed or joiner) configured via `CRAFTY_*` env.
pub struct RunOpts {
    /// In-memory transport for integration tests only ([`RunOpts::local`]).
    pub(crate) local_net: Option<LocalNetwork>,
    /// Poll for leader / optional queue mount after boot.
    pub wait_ready: Option<ReadyOpts>,
    /// Graceful teardown after the signal.
    pub shutdown: ShutdownOpts,
}

impl Default for RunOpts {
    fn default() -> Self {
        Self {
            local_net: None,
            wait_ready: None,
            shutdown: ShutdownOpts::from_env(),
        }
    }
}

impl RunOpts {
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
}

impl ShutdownOpts {
    /// Read `CRAFTY_GRACEFUL_LEAVE` (defaults to `true` in product mode).
    #[must_use]
    pub fn from_env() -> Self {
        CraftyApp::shutdown_opts_from_env()
    }
}
