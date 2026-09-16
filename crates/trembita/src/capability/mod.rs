//! Capability DX — typed ops and routes (B-21).
//!
//! App authors register [`CapGroup`] + [`CapOp`] in [`CapManifest`], implement handlers as
//! plain async fns, and call via [`CapRequest::via`].

mod apply;
mod call;
mod ctx;
mod deps;
mod error;
pub(crate) mod event;
pub(crate) mod group;
pub(crate) mod host;
mod ingress;
mod manifest;
mod op;
pub(crate) mod queue;
mod route;
pub(crate) mod runtime;
mod wait;
mod wire;

pub use call::{
    CallBuilder, CapCallOpts, CapEnqueueOutcome, CapRequest, CapVia, enqueue, fire, invoke,
    publish_event,
};
pub use ctx::OpCtx;
pub use deps::CapDeps;
pub use error::CapError;
pub use event::deliver_event;
pub use group::{CapGroup, CapGroupScale};
pub use ingress::CapIngress;
pub use manifest::CapManifest;
pub use op::CapOp;
pub use queue::deliver_queued;
pub use route::Route;
pub use runtime::CapRuntime;
pub use wire::{CapQueued, CapWire};

pub(crate) use call::cap_wire_bytes;

pub(crate) use apply::CapGroupApply;
