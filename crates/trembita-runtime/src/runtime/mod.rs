//! The node runtime — an async event loop (spawned by [`spawn`]) that turns a
//! [`RaftDriver`] into a live, networked node (backlog E1/E2/E4).
//!
//! [`RaftDriver`] is synchronous and I/O-free: it must be *driven*. This module
//! supplies the drive train:
//!
//! * A **tokio task** owns the driver and selects over a periodic tick (the
//!   election/heartbeat clock, E2) and an inbound mailbox.
//! * Outbound [`NetEffect::Send`](crate::NetEffect)s are dispatched over a
//!   [`trembita_net`] [`Transport`]; each peer reply is fed back into the mailbox,
//!   so request/response transports drive the core's fire-and-forget model.
//! * Client **proposals** and **queries** are correlated to their results:
//!   a proposal's `oneshot` responder is keyed by the log index it lands at and
//!   fired when that index applies; a query's responder is keyed by its
//!   [`ReadId`] and fired when the `ReadIndex` round confirms.
//! * A [`NodeService`] adapter implements [`trembita_net`]'s [`RequestHandler`] so
//!   a `QuicServer` (or the in-memory `LocalNetwork`) can route inbound
//!   `/peer/wire` and `/client/wire` requests into the running node.
//!
//! The loop holds an `Arc<dyn Transport>`, so the exact same runtime runs over
//! the deterministic `LocalNetwork` in tests and over live QUIC in production
//! (wire-transport) with no code changes.
//!
//! ## Not yet wired (tracked in the backlog)
//!
//! * **Durable persistence** (B4): the in-memory core log is the source of
//!   truth; hard state and the log are not yet flushed through `trembita-storage`,
//!   so a restart loses state.
//! * **Log compaction / snapshots** (Track G): leaders can compact via
//!   [`NodeHandle::compact`]; inbound `InstallSnapshot` restore is handled via
//!   the driver. Automatic background compaction is not wired yet.
//! * **Per-connection identity** (C5): [`NodeService`] trusts the sender id
//!   declared inside a peer RPC instead of the presented client certificate.
//! * **Fatal errors are silent**: a corrupt-log / state-machine failure stops
//!   the loop with no diagnostic until `tracing` lands (Track H).

mod event_loop;
mod handle;
mod service;
mod spawn;
mod types;
mod wire;

pub use handle::NodeHandle;
pub use service::NodeService;
pub use spawn::spawn;
pub use types::{
    ClientError, NodeStatus, QueueAutoscalePolicyAppliedFn, RuntimeConfig, SagaJournalAppliedFn,
    TwoPhaseGcAbortedFn, TwoPhaseJournalAppliedFn,
};
