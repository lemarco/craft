//! Transactional domain outbox (B-52) — **your** DB transaction writes rows; trembita drains to
//! [`EventTopic`](https://docs.rs/trembita/latest/trembita/struct.TopicOpts.html) on the Raft leader.
//!
//! Product wiring ([event-outbox.md]({{TREMBITA_DOC_BASE}}/docs/decisions/event-outbox.md)):
//!
//! ```ignore
//! use std::sync::Arc;
//! use trembita::{EventOutboxDrainOpts, TopicOpts};
//!
//! TrembitaApp::builder()
//!     .topics([TopicOpts::topic("platform.events")
//!         .subscriptions(["analytics"])
//!         .outbox(Arc::new(my_outbox), EventOutboxDrainOpts::default())])
//! ```
//!
//! Postgres adapter: enable crate feature `domain-outbox` and [`PgEventOutboxSource`](https://docs.rs/trembita/latest/trembita/struct.PgEventOutboxSource.html).
//! Custom stores implement [`EventOutboxSource`](https://docs.rs/trembita/latest/trembita_events/event_outbox/trait.EventOutboxSource.html) in this module.

/// Scaffold placeholder — replace with `EventOutboxSource` + SQL migrations in your domain layer.
pub const OUTBOX_IMPLEMENTATION_HINT: &str =
    "Write outbox rows in the same transaction as domain updates; wire TopicOpts::outbox in manifest.";
