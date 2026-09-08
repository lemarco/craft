//! Job enqueue and operator routes (`/jobs/*`).

use std::sync::Arc;

use trembita::{TrembitaApp, TrembitaGatewayState};
use trembita_http::{AuthMode, RouteTable};

/// Job API route table (identity-protected when gateway identity is configured).
#[must_use]
pub fn route_table(state: &TrembitaGatewayState) -> RouteTable {
    TrembitaApp::jobs_api(Arc::clone(&state.app))
        .route_table()
        .with_auth_mode(AuthMode::Identity)
}
