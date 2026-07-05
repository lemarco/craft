//! `craft-storage` — durable log, hard-state, and snapshot stores.
//!
//! Defines the storage traits with an in-memory implementation for tests and a
//! `redb` implementation for production (backlog Track B).

pub use craft_proto as proto;
