//! Internal: env-boot gateway surface closure.

use std::sync::Arc;

use crate::gateway::GatewaySurfacesFn;

use super::gateway::DefaultGatewayApis;
use super::runtime::TrembitaApp;

#[must_use]
pub(super) fn default_product_surfaces(
    apis: DefaultGatewayApis,
    extra_routes: Option<
        Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
    websocket_routes: Option<
        Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
) -> GatewaySurfacesFn {
    Box::new(move |state| {
        trembita_http::Gateway::new(false).dev_fallback(product_routes(
            &state,
            apis,
            extra_routes.as_ref(),
            websocket_routes.as_ref(),
        ))
    })
}

/// Merge registration-driven product routes into a custom gateway surface.
#[must_use]
pub(super) fn composite_product_surfaces(
    apis: DefaultGatewayApis,
    extra_routes: Option<
        Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
    websocket_routes: Option<
        Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
    user_surfaces: GatewaySurfacesFn,
) -> GatewaySurfacesFn {
    Box::new(move |state| {
        let routes = product_routes(
            &state,
            apis,
            extra_routes.as_ref(),
            websocket_routes.as_ref(),
        );
        user_surfaces(state).merge_routes(routes)
    })
}

fn product_routes(
    state: &crate::gateway::TrembitaGatewayState,
    apis: DefaultGatewayApis,
    extra_routes: Option<
        &Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
    websocket_routes: Option<
        &Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
) -> trembita_http::RouteTable {
    let mut table = TrembitaApp::default_product_routes(state, apis);
    if let Some(extra) = extra_routes {
        table = table.merge(extra(state.clone()));
    }
    if let Some(ws) = websocket_routes {
        table = table.merge(ws(state.clone()));
    }
    table
}
