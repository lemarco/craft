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
) -> GatewaySurfacesFn {
    Box::new(move |state| {
        let mut gateway = TrembitaApp::default_surfaces(state.clone(), false, apis);
        if let Some(extra) = &extra_routes {
            gateway = gateway.merge_routes(extra(state));
        }
        gateway
    })
}
