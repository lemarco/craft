use std::time::Duration;

use tokio::task::JoinHandle;

/// Options for [`super::TrembitaApp::shutdown_graceful`] and [`super::TrembitaAppBuilder::run`].
#[derive(Debug)]
pub struct ShutdownOpts {
    /// Call [`TrembitaCluster::leave`](crate::cluster::TrembitaCluster::leave) when the node is in a multi-node cluster.
    pub graceful_leave: bool,
    /// Drain local actor groups before stopping the runtime.
    pub drain_actors: bool,
    /// Stop job consumers: send on the watch sender, then await these handles.
    pub consumers: Option<(tokio::sync::watch::Sender<bool>, Vec<JoinHandle<()>>)>,
    /// Drain the product HTTP gateway (WebSocket / long-lived HTTP) before shutdown.
    pub drain_gateway: bool,
    /// Max wait for job queue consumer tasks after the stop signal.
    pub consumer_drain_timeout: Duration,
}

impl Default for ShutdownOpts {
    fn default() -> Self {
        Self {
            graceful_leave: true,
            drain_actors: true,
            consumers: None,
            drain_gateway: true,
            consumer_drain_timeout: crate::gateway::DEFAULT_CONSUMER_DRAIN_TIMEOUT,
        }
    }
}
