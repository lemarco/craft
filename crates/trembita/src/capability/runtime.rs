//! Runtime metadata built from [`super::CapManifest`].

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use super::route::Route;
use super::wait::CapWaitStore;
use crate::TrembitaApp;

pub(crate) use super::op::KeyFn;

/// Runtime metadata for one registered op (routes, queue stream, key fn).
#[derive(Clone)]
pub(crate) struct OpBinding {
    pub group: &'static str,
    pub op: &'static str,
    pub routes: Vec<Route>,
    pub queue_stream: Option<&'static str>,
    pub event_topic: Option<&'static str>,
    pub key: Option<KeyFn>,
}

/// Lookup table for [`super::call::invoke`] and gateway adapters.
#[derive(Clone)]
pub struct CapRuntime {
    ops: HashMap<(String, String), OpBinding>,
    wait: Arc<CapWaitStore>,
    app_slot: Arc<OnceLock<Weak<TrembitaApp>>>,
}

impl Default for CapRuntime {
    fn default() -> Self {
        Self {
            ops: HashMap::new(),
            wait: Arc::new(CapWaitStore::default()),
            app_slot: Arc::new(OnceLock::new()),
        }
    }
}

impl CapRuntime {
    /// Empty runtime — used before any [`super::CapManifest`] is applied.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, binding: OpBinding) {
        self.ops
            .insert((binding.group.to_string(), binding.op.to_string()), binding);
    }

    pub(crate) fn binding(&self, group: &str, op: &str) -> Option<&OpBinding> {
        self.ops.get(&(group.to_string(), op.to_string()))
    }

    pub(crate) fn wait_store(&self) -> Arc<CapWaitStore> {
        Arc::clone(&self.wait)
    }

    pub(crate) fn app_slot(&self) -> Arc<OnceLock<Weak<TrembitaApp>>> {
        Arc::clone(&self.app_slot)
    }

    pub(crate) fn attach_app(&self, app: Weak<TrembitaApp>) {
        let _ = self.app_slot.set(app);
    }
}
