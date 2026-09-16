//! Advanced [`UserActor`](trembita::runtime::UserActor) groups — migration labs, realtime mailboxes.
//!
//! Default product ops belong in [`crate::capabilities`] + [`CapManifest`](trembita::CapManifest).
//! Register workers in `src/manifest.rs` (`// trembita:workers`) and opt in to `/actors/*` HTTP with
//! [`WorkerOpts::http_cast(true)`](trembita::WorkerOpts::http_cast).

// Example:
// use trembita::{actor, runtime::UserActor, WorkerOpts, WorkerScale, workers};
//
// pub struct CatalogWorker;
//
// #[actor]
// impl UserActor for CatalogWorker { ... }
