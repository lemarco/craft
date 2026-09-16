//! [`TrembitaClusterBuilder`] — ergonomic node assembly (consensus, actors, gateway, product APIs).

mod autoscale;
mod cluster;
mod error;
mod join;

pub(crate) use cluster::TrembitaClusterBuilder;
pub use error::StartError;
