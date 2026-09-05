//! PostgreSQL [`EventOutboxSource`](trembita_events::EventOutboxSource) adapter.
//!
//! Poll unpublished rows with `FOR UPDATE SKIP LOCKED` semantics delegated to the
//! leader drainer in `trembita-events`; this crate implements `poll` + `mark_published` only.

mod schema;
mod source;

pub use schema::PgEventOutboxSchema;
pub use source::PgEventOutboxSource;
