//! Runtime / cluster tuning for [`CraftyAppBuilder`](super::app::CraftyAppBuilder).

use std::net::SocketAddr;
use std::time::Duration;

use crafty_core::Config;

use crate::NodeId;
use crate::app::EmptyStateMachine;
use crate::builder::CraftyClusterBuilder;

/// Cluster runtime options for [`.configure`](super::app::CraftyAppBuilder::configure).
///
/// [`Default`] matches the framework baseline (same as a fresh [`CraftyClusterBuilder`]).
/// Override only what you need:
///
/// ```
/// use std::time::Duration;
/// use crafty::{CraftyApp, CraftyConfigure};
///
/// let _builder = CraftyApp::builder().configure(CraftyConfigure {
///     tick_period: Duration::from_millis(10),
///     admin_addr: Some("127.0.0.1:8080".parse().expect("addr")),
///     ..CraftyConfigure::default()
/// });
/// ```
#[derive(Debug, Clone)]
pub struct CraftyConfigure {
    /// Override node id before boot (joiners usually leave unset).
    pub node_id: Option<NodeId>,
    /// Raft election / heartbeat tuning.
    pub raft_config: Config,
    /// Wall-clock duration of one logical Raft tick.
    pub tick_period: Duration,
    /// Leader supervisor reconcile interval.
    pub reconcile_period: Duration,
    /// Actor directory publish interval.
    pub directory_publish_period: Duration,
    /// Admin dashboard + `/metrics` bind. `None` = disabled unless `CRAFTY_ADMIN` at boot.
    pub admin_addr: Option<SocketAddr>,
}

impl Default for CraftyConfigure {
    fn default() -> Self {
        Self {
            node_id: None,
            raft_config: Config::default(),
            tick_period: Duration::from_millis(50),
            reconcile_period: Duration::from_millis(250),
            directory_publish_period: Duration::from_millis(250),
            admin_addr: None,
        }
    }
}

impl CraftyConfigure {
    /// Apply to a cluster builder (used by [`CraftyAppBuilder::configure`]).
    #[must_use]
    pub(crate) fn apply_to(
        self,
        inner: CraftyClusterBuilder<EmptyStateMachine>,
    ) -> CraftyClusterBuilder<EmptyStateMachine> {
        let mut inner = if let Some(node_id) = self.node_id {
            CraftyClusterBuilder::new(node_id, EmptyStateMachine)
        } else {
            inner
        };
        inner = inner
            .raft_config(self.raft_config)
            .tick_period(self.tick_period)
            .reconcile_period(self.reconcile_period)
            .directory_publish_period(self.directory_publish_period);
        if let Some(addr) = self.admin_addr {
            inner = inner.admin_addr(addr);
        }
        inner
    }
}
