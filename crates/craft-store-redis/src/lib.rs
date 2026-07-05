//! `craft-store-redis` — Redis implementation of `ActorStateStore` (ADR 021).
//!
//! Optional crate for externalizing stateful-actor data (backlog Track G),
//! tested against a real Redis via `testcontainers` (ADR 029).

pub use craft_actor;
