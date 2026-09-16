//! Capability group — cluster pool hosting shared state and ops.

use std::sync::{Arc, OnceLock, Weak};

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::call::CapRequest;
use super::host::{CapHost, CapHostConfig, CapRegistry};
use super::op::{CapHandlerFn, CapOp, CapOpSpec};
use super::route::Route;
use crate::TrembitaApp;

/// How many capability host actor instances run for a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapGroupScale {
    /// Fixed pool size cluster-wide.
    Fixed(usize),
    /// One host per live cluster node.
    PerNode,
}

/// A named capability group (bounded context) on the cluster.
pub struct CapGroup<S: Send + Default + 'static = ()> {
    name: &'static str,
    scale: CapGroupScale,
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
    /// Group name from [`CapRequest::GROUP`] on `Req`.
    #[must_use]
    pub fn for_cap<Req: CapRequest>() -> Self {
        Self::with_state(Req::GROUP)
    }

    /// Group with explicit shared state type `S`.
    #[must_use]
    pub fn with_state(name: &'static str) -> Self {
        Self {
            name,
            scale: CapGroupScale::Fixed(1),
            queue_stream: None,
            event_topic: None,
            event_subscription: None,
            ops: Vec::new(),
        }
    }

    /// Fixed host count cluster-wide (default `1`).
    #[must_use]
    pub fn instances(mut self, n: usize) -> Self {
        self.scale = CapGroupScale::Fixed(n.max(1));
        self
    }

    /// One capability host per live cluster node (realtime / stateful pools).
    #[must_use]
    pub fn per_node(mut self) -> Self {
        self.scale = CapGroupScale::PerNode;
        self
    }

    /// Job stream used for [`super::Route::Queued`] on ops in this group.
    #[must_use]
    pub fn queue_stream(mut self, stream: &'static str) -> Self {
        self.queue_stream = Some(stream);
        self
    }

    /// [`CapRequest::QUEUE_STREAM`] (`{group}.{op}`) for macro-generated request types.
    #[must_use]
    pub fn default_queue_for<Req: CapRequest>(self) -> Self {
        self.queue_stream(Req::QUEUE_STREAM)
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

    /// [`CapRequest::EVENT_TOPIC`] + [`CapRequest::EVENT_SUBSCRIPTION`] for macro-generated ops.
    #[must_use]
    pub fn default_event_ingress_for<Req: CapRequest>(self) -> Self {
        self.event_ingress(Req::EVENT_TOPIC, Req::EVENT_SUBSCRIPTION)
    }

    /// Register an operation.
    #[must_use]
    pub fn op(mut self, op: CapOp<S>) -> Self {
        self.ops.push(op.into_spec());
        self
    }

    /// Register a sync handler for [`CapRequest`] type `Req` (op name from `Req::OP`).
    #[must_use]
    pub fn op_req<Req, Reply>(
        self,
        handler: CapHandlerFn<S, Req, Reply>,
        routes: impl IntoIterator<Item = Route>,
    ) -> Self
    where
        Req: CapRequest<Reply = Reply> + DeserializeOwned + Send + 'static,
        Reply: Serialize + Send + 'static,
    {
        self.op(CapOp::for_request(handler).routes(routes))
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

    pub(crate) fn scale(&self) -> CapGroupScale {
        self.scale
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
