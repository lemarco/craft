//! Capability group — cluster pool hosting shared state and ops.

use std::sync::{Arc, OnceLock, Weak};

use super::host::{CapHost, CapHostConfig, CapRegistry};
use super::op::{CapOp, CapOpSpec};
use crate::TrembitaApp;

/// A named capability group (bounded context) on the cluster.
pub struct CapGroup<S: Send + Default + 'static = ()> {
    name: &'static str,
    instances: usize,
    queue_stream: Option<&'static str>,
    event_topic: Option<&'static str>,
    event_subscription: Option<&'static str>,
    ops: Vec<CapOpSpec<S>>,
}

impl CapGroup<()> {
    /// Stateless group (default state unit type).
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self::with_state(name)
    }
}

impl<S: Send + Default + 'static> CapGroup<S> {
    /// Group with explicit shared state type `S`.
    #[must_use]
    pub fn with_state(name: &'static str) -> Self {
        Self {
            name,
            instances: 1,
            queue_stream: None,
            event_topic: None,
            event_subscription: None,
            ops: Vec::new(),
        }
    }

    /// Worker instances cluster-wide (default `1`).
    #[must_use]
    pub fn instances(mut self, n: usize) -> Self {
        self.instances = n.max(1);
        self
    }

    /// Job stream used for [`super::Route::Queued`] on ops in this group.
    #[must_use]
    pub fn queue_stream(mut self, stream: &'static str) -> Self {
        self.queue_stream = Some(stream);
        self
    }

    /// Durable topic + subscription for [`super::Route::Event`] on ops in this group.
    ///
    /// Published payloads are postcard-encoded [`super::CapWire`] frames.
    #[must_use]
    pub fn event_ingress(mut self, topic: &'static str, subscription: &'static str) -> Self {
        self.event_topic = Some(topic);
        self.event_subscription = Some(subscription);
        self
    }

    /// Register an operation.
    #[must_use]
    pub fn op(mut self, op: CapOp<S>) -> Self {
        self.ops.push(op.into_spec());
        self
    }

    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn build_registry(&self) -> CapRegistry<S> {
        let mut registry = CapRegistry::empty();
        for spec in &self.ops {
            spec.install(&mut registry);
        }
        registry
    }

    pub(crate) fn host_config(
        &self,
        app_slot: Arc<OnceLock<Weak<TrembitaApp>>>,
    ) -> CapHostConfig<S> {
        CapHostConfig {
            registry: self.build_registry().arc(),
            app_slot,
        }
    }

    pub(crate) fn ops(&self) -> &[CapOpSpec<S>] {
        &self.ops
    }

    pub(crate) fn instances_count(&self) -> usize {
        self.instances
    }

    pub(crate) fn queued_stream(&self) -> Option<&'static str> {
        self.queue_stream
    }

    pub(crate) fn event_ingress_spec(&self) -> Option<(&'static str, &'static str)> {
        match (self.event_topic, self.event_subscription) {
            (Some(t), Some(s)) => Some((t, s)),
            _ => None,
        }
    }
}

/// Internal host actor (not part of the app author API).
pub(crate) type CapHostActor<S> = CapHost<S>;
