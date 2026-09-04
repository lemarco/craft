//! [`TrembitaCluster`] — the running node handle returned by the builder.
//!
//! It bundles everything the facade wired together: the consensus/actor runtime
//! (via an in-process [`NodeHandle`] for zero-copy L1 clients), the actor
//! control/messaging/directory planes, the leader-only supervisor, and the
//! telemetry [`EventBus`] + [`Metrics`]. Background tasks (facts refresh,
//! directory anti-entropy, supervisor reconcile, admin server) run until
//! [`shutdown`](TrembitaCluster::shutdown) or the handle is dropped.

mod cluster;
mod errors;
mod facts;
mod telemetry;

#[cfg(test)]
mod tests;

pub use cluster::TrembitaCluster;
pub use errors::{AddRaftGroupsError, LeaveError, ScaleClusterError};
pub use facts::ClusterFacts;
pub(crate) use telemetry::{ActorTelemetry, MembershipTelemetry};
