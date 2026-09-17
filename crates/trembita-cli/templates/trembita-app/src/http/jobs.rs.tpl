//! Job enqueue and operator routes (`/jobs/*`).

use std::sync::Arc;

use trembita::{AuthMode, TrembitaApp, TrembitaGatewayState};
use trembita::RouteTable;

/// Job API route table (identity-protected when gateway identity is configured).
#[allow(dead_code)] // optional brownfield merge — `.jobs()` + `from_config` mounts `/jobs/*`
#[must_use]
pub fn route_table(state: &TrembitaGatewayState) -> RouteTable {
    TrembitaApp::jobs_api(Arc::clone(&state.app))
        .route_table()
        .with_auth_mode(AuthMode::Identity)
}
