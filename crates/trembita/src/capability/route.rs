//! Invocation routes (delivery mode), chosen at the call site.
//!
//! Ops registered without [`.routes`](super::CapOp::routes) allow **every** [`Route`]; the caller
//! picks the mode (`.via(&app).route(…)`, `fire_cap`, session API, …). Queued/event still need
//! group [`.queue_stream`](super::CapGroup::queue_stream) / [`.event_ingress`](super::CapGroup::event_ingress).

use std::fmt;

/// How a capability op is invoked — not part of the request type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    /// Await a reply via cluster ask (keyed when a key fn is registered).
    Inline,
    /// Cast without waiting for a reply.
    InlineFire,
    /// Enqueue on the group's queue stream (at-least-once).
    Queued,
    /// Enqueue, then await the handler reply stored when the consumer finishes.
    QueuedWait,
    /// Enqueue for a future run (`CallBuilder::run_at_ms`).
    Scheduled,
    /// Deliver via sticky [`crate::ActorSession`] (`CallBuilder::session_key`).
    Session,
    /// Topic ingress (subscribe) or egress (publish via [`super::publish_event`] / [`.publish_event()`](super::CallBuilder::publish_event)).
    Event,
}

impl fmt::Display for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline => f.write_str("inline"),
            Self::InlineFire => f.write_str("inline_fire"),
            Self::Queued => f.write_str("queued"),
            Self::QueuedWait => f.write_str("queued_wait"),
            Self::Scheduled => f.write_str("scheduled"),
            Self::Session => f.write_str("session"),
            Self::Event => f.write_str("event"),
        }
    }
}
