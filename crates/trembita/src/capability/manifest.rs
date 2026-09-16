//! [`CapManifest`] — registered capability groups.

use crate::TrembitaAppBuilder;

use super::apply::CapGroupApply;
use super::group::CapGroup;
use super::runtime::CapRuntime;

/// Registry of capability groups — wire via [`crate::AppManifest::capabilities`].
pub struct CapManifest {
    pub(crate) groups: Vec<Box<dyn CapGroupApply>>,
}

impl Default for CapManifest {
    fn default() -> Self {
        Self { groups: Vec::new() }
    }
}

impl CapManifest {
    /// Empty manifest — chain [`.group`](Self::group).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a capability group.
    #[must_use]
    pub fn group<S: Send + Default + 'static>(mut self, group: CapGroup<S>) -> Self {
        self.groups.push(Box::new(group));
        self
    }

    /// Whether any groups are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Apply groups to a builder and produce runtime metadata for [`crate::TrembitaApp`].
    #[must_use]
    pub fn apply(self, builder: TrembitaAppBuilder) -> (TrembitaAppBuilder, CapRuntime) {
        crate::app::capability_wiring::wire_manifest(builder, self.groups)
    }
}
