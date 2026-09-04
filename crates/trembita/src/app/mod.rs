//! [`TrembitaApp`] — product-facing entry point over [`TrembitaCluster`](crate::cluster_handle::TrembitaCluster)
//! ([product-scenarios](../../../docs/decisions/product-scenarios.md)).

mod builder;
mod runtime;
mod shutdown;
mod types;
mod workflow;

pub use builder::TrembitaAppBuilder;
pub use runtime::TrembitaApp;
pub use shutdown::ShutdownOpts;
pub use types::{EmptyStateMachine, WorkerInfo};
pub use workflow::journal_workflow;
