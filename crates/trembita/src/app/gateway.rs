//! Default product gateway wiring ([`TrembitaApp::default_surfaces`]).

use std::sync::Arc;

use trembita_http::{AuthMode, Gateway, RouteTable};

use crate::gateway::TrembitaGatewayState;

use super::runtime::TrembitaApp;

/// Which built-in HTTP APIs to mount on the default gateway surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultGatewayApis {
    /// `/health`, `/ready`, `/metrics`, `/dashboard`, `/introspect/*` (on by default).
    pub ops: bool,
    /// `POST /jobs/{stream}` and operator job routes.
    pub jobs: bool,
    /// `GET/PUT/DELETE /jobs/{stream}/schedules` (requires `jobs`).
    pub schedules: bool,
    /// Actor cast/ask HTTP API.
    pub actors: bool,
    /// Workflow trigger HTTP API.
    pub workflows: bool,
    /// Event topic publish + metrics HTTP API.
    pub topics: bool,
}

impl Default for DefaultGatewayApis {
    fn default() -> Self {
        Self {
            ops: true,
            jobs: false,
            schedules: false,
            actors: false,
            workflows: false,
            topics: false,
        }
    }
}

impl DefaultGatewayApis {
    /// Ops routes only (no jobs/actors/workflows/topics product APIs).
    #[must_use]
    pub const fn ops_only() -> Self {
        Self {
            ops: true,
            jobs: false,
            schedules: false,
            actors: false,
            workflows: false,
            topics: false,
        }
    }
}

impl TrembitaApp {
    /// Ops + optional jobs/actors/workflows route tables (identity on product APIs).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn default_product_routes(
        state: &TrembitaGatewayState,
        apis: DefaultGatewayApis,
    ) -> RouteTable {
        let mut table = if apis.ops {
            state.app.ops_api().route_table()
        } else {
            RouteTable::new()
        };
        if apis.jobs {
            table = table.merge(
                Self::jobs_api(Arc::clone(&state.app))
                    .route_table()
                    .with_auth_mode(AuthMode::Identity),
            );
        }
        if apis.schedules {
            table = table.merge(
                Self::schedules_api(Arc::clone(&state.app))
                    .route_table()
                    .with_auth_mode(AuthMode::Identity),
            );
        }
        if apis.actors {
            table = table.merge(
                Self::actors_api(Arc::clone(&state.app))
                    .route_table()
                    .with_auth_mode(AuthMode::Identity),
            );
        }
        if apis.workflows {
            table = table.merge(
                Self::workflows_api(Arc::clone(&state.app))
                    .route_table()
                    .with_auth_mode(AuthMode::Identity),
            );
        }
        if apis.topics {
            table = table.merge(
                Self::topics_api(Arc::clone(&state.app))
                    .route_table()
                    .with_auth_mode(AuthMode::Identity),
            );
        }
        table
    }

    /// Default local-dev gateway: built-in routes on loopback [`Gateway::dev_fallback`].
    ///
    /// Host split (production): chain [`.surface_hosts`](Gateway::surface_hosts) on the returned
    /// [`Gateway`] — see [gateway-routing-v2](../../../docs/decisions/gateway-routing-v2.md).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn default_surfaces(
        state: TrembitaGatewayState,
        is_production: bool,
        apis: DefaultGatewayApis,
    ) -> Gateway {
        Gateway::new(is_production).dev_fallback(Self::default_product_routes(&state, apis))
    }
}
