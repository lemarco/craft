//! Runtime / cluster tuning for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::net::SocketAddr;
use std::time::Duration;

use trembita_core::Config;

use crate::NodeId;
use crate::app::EmptyStateMachine;
use crate::builder::TrembitaClusterBuilder;

/// Cluster runtime options for [`.configure`](super::app::TrembitaAppBuilder::configure).
///
/// [`Default`] matches the framework baseline (same as a fresh [`TrembitaClusterBuilder`]).
/// Override only what you need:
///
/// ```
/// use std::time::Duration;
/// use trembita::{TrembitaApp, TrembitaConfigure};
///
/// let _builder = TrembitaApp::builder().configure(TrembitaConfigure {
///     tick_period: Duration::from_millis(10),
///     admin_addr: Some("127.0.0.1:8080".parse().expect("addr")),
///     ..TrembitaConfigure::default()
/// });
/// ```
#[derive(Debug, Clone)]
pub struct TrembitaConfigure {
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
    /// Admin dashboard + `/metrics` bind. `None` = disabled unless `TREMBITA_ADMIN` at boot.
    pub admin_addr: Option<SocketAddr>,
}

impl Default for TrembitaConfigure {
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

impl TrembitaConfigure {
    /// Apply to a cluster builder (used by [`TrembitaAppBuilder::configure`]).
    #[must_use]
    pub(crate) fn apply_to(
        self,
        inner: TrembitaClusterBuilder<EmptyStateMachine>,
    ) -> TrembitaClusterBuilder<EmptyStateMachine> {
        let mut inner = if let Some(node_id) = self.node_id {
            TrembitaClusterBuilder::new(node_id, EmptyStateMachine).with_explicit_node_id()
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
