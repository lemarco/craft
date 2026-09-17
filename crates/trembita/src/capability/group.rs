//! Capability group — cluster pool hosting shared state and ops.

use std::sync::{Arc, OnceLock, Weak};

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::call::CapRequest;
use super::host::{CapHost, CapHostConfig, CapRegistry};
use super::op::{CapHandlerFn, CapOp, CapOpSpec};
use super::route::Route;
use crate::TrembitaApp;

fn op_declares_session(routes: &[Route]) -> bool {
    routes.iter().any(|r| matches!(r, Route::Session))
}

fn group_declares_session<S: Send + Default + 'static>(ops: &[CapOpSpec<S>]) -> bool {
    ops.iter().any(|spec| op_declares_session(&spec.routes))
}

/// Shared RAM in the group host (non–zero-sized `State`); use explicit `.instances(n)` or accept `Fixed(1)` auto default.
fn state_holds_shared_ram<S: Send + Default + 'static>() -> bool {
    std::mem::size_of::<S>() > 0
}

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
    /// `None` → resolved at manifest apply ([`resolved_scale`](Self::resolved_scale)).
    scale: Option<CapGroupScale>,
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
    ///
    /// Host scale is resolved at manifest apply unless you call [`.instances`](Self::instances) or
    /// [`.per_node`](Self::per_node) (marker-only `State` → [`CapGroupScale::PerNode`], shared RAM
    /// or any `Route::Session` op → [`CapGroupScale::Fixed`]).
    #[must_use]
    pub fn with_state(name: &'static str) -> Self {
        Self {
            name,
            scale: None,
            queue_stream: None,
            event_topic: None,
            event_subscription: None,
            ops: Vec::new(),
        }
    }

    /// Fixed host count cluster-wide (opts out of automatic scale).
    #[must_use]
    pub fn instances(mut self, n: usize) -> Self {
        self.scale = Some(CapGroupScale::Fixed(n.max(1)));
        self
    }

    /// One capability host per live cluster node (realtime / stateful pools).
    #[must_use]
    pub fn per_node(mut self) -> Self {
        self.scale = Some(CapGroupScale::PerNode);
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
            group: self.name,
            registry: self.build_registry().arc(),
            app_slot,
        }
    }

    pub(crate) fn ops(&self) -> &[CapOpSpec<S>] {
        &self.ops
    }

    pub(crate) fn resolved_scale(&self) -> CapGroupScale {
        if let Some(scale) = self.scale {
            return scale;
        }
        if group_declares_session(self.ops()) {
            return CapGroupScale::Fixed(1);
        }
        if state_holds_shared_ram::<S>() {
            return CapGroupScale::Fixed(1);
        }
        CapGroupScale::PerNode
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::CapError;
    use crate::capability::OpCtx;

    #[derive(Default)]
    struct MarkerState;

    #[derive(Default)]
    struct RamState {
        _n: u64,
    }

    fn marker_op(_: (), _ctx: OpCtx<'_>, _state: &mut MarkerState) -> Result<(), CapError> {
        Ok(())
    }

    fn ram_op(_: (), _ctx: OpCtx<'_>, _state: &mut RamState) -> Result<(), CapError> {
        Ok(())
    }

    #[test]
    fn auto_scale_marker_state_without_session_is_per_node() {
        let group = CapGroup::<MarkerState>::with_state("ping")
            .op(CapOp::new("ping", marker_op).routes([Route::Inline, Route::Queued]));
        assert_eq!(group.resolved_scale(), CapGroupScale::PerNode);
    }

    #[test]
    fn auto_scale_shared_ram_state_is_fixed_one() {
        let group = CapGroup::<RamState>::with_state("math")
            .op(CapOp::new("add", ram_op).routes([Route::Inline]));
        assert_eq!(group.resolved_scale(), CapGroupScale::Fixed(1));
    }

    #[test]
    fn auto_scale_session_route_is_fixed_one() {
        let group = CapGroup::<MarkerState>::with_state("rt")
            .op(CapOp::new("append", marker_op).routes([Route::Session]));
        assert_eq!(group.resolved_scale(), CapGroupScale::Fixed(1));
    }

    #[test]
    fn instances_overrides_auto_scale() {
        let group = CapGroup::<MarkerState>::with_state("ping")
            .instances(3)
            .op(CapOp::new("ping", marker_op).routes([Route::Inline]));
        assert_eq!(group.resolved_scale(), CapGroupScale::Fixed(3));
    }

    #[test]
    fn per_node_overrides_auto_scale() {
        let group = CapGroup::<RamState>::with_state("rt")
            .per_node()
            .op(CapOp::new("append", ram_op).routes([Route::Session]));
        assert_eq!(group.resolved_scale(), CapGroupScale::PerNode);
    }

    /// B-31 — runtime defaults align with founder scale map (doctor catches manifest foot-guns).
    #[test]
    fn founder_scale_b31_runtime_defaults_table() {
        let marker_queued = CapGroup::<MarkerState>::with_state("email")
            .op(CapOp::new("deliver", marker_op).routes([Route::Queued]));
        assert_eq!(
            marker_queued.resolved_scale(),
            CapGroupScale::PerNode,
            "stateless queued: handlers scale PerNode"
        );

        let marker_pinned = CapGroup::<MarkerState>::with_state("email")
            .instances(1)
            .op(CapOp::new("deliver", marker_op).routes([Route::Queued]));
        assert_eq!(
            marker_pinned.resolved_scale(),
            CapGroupScale::Fixed(1),
            "explicit Fixed(1) overrides PerNode default"
        );

        let session_fixed = CapGroup::<MarkerState>::with_state("chat")
            .op(CapOp::new("append", marker_op).routes([Route::Session]));
        assert_eq!(
            session_fixed.resolved_scale(),
            CapGroupScale::Fixed(1),
            "session ops default Fixed(1) until .per_node()"
        );
    }
}
