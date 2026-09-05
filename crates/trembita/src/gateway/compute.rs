//! Gateway middleware — acquire a compute token for each request.

use trembita_runtime::ComputeTokenPool;

/// Compute token pool wired into [`super::router::WrappedGatewayService`].
pub use trembita_runtime::ComputeTokenPool as GatewayComputePool;
