//! Actor group registration options for [`CraftyAppBuilder`](super::app::CraftyAppBuilder).

/// Scale and config for [`.actors`](super::app::CraftyAppBuilder::actors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorGroupOpts<C> {
    /// Actor constructor config passed to [`UserActor`](crafty_actor::UserActor).
    pub config: C,
    /// `None` — one instance per live cluster node ([`manage_auto`](crate::cluster::CraftyClusterBuilder::manage_auto)).
    /// `Some(n)` — fixed pool of `n` instances cluster-wide ([`manage`](crate::cluster::CraftyClusterBuilder::manage)).
    pub total: Option<usize>,
}

impl<C> ActorGroupOpts<C> {
    /// Auto scale: one worker per live node.
    #[must_use]
    pub fn new(config: C) -> Self {
        Self {
            config,
            total: None,
        }
    }

    /// Fixed pool size across the cluster.
    #[must_use]
    pub fn fixed(config: C, total: usize) -> Self {
        Self {
            config,
            total: Some(total),
        }
    }
}

impl<C: Default> Default for ActorGroupOpts<C> {
    fn default() -> Self {
        Self::new(C::default())
    }
}
