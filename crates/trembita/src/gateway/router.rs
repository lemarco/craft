use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;

use super::super::app::TrembitaApp;
use super::config::{
    GATEWAY_MAX_BODY_BYTES, GatewayConfig, GatewayConfigError, validate_gateway_config,
};
use super::drain::{self, ConnectionTracker};
use super::identity::{self, GatewayBearerIdentity, GatewayRequest};
use super::rate_limit;
use super::state::TrembitaGatewayState;

/// Bearer auth hook for product/upgrade HTTP when a gateway token env var is set.
#[must_use]
pub fn bearer_auth_from_env() -> Option<trembita_http::AuthFn> {
    super::config::gateway_token_from_env()
        .map(|_| identity_auth_fn(identity::erase_identity(GatewayBearerIdentity::from_env())))
}

/// Build the gateway router: custom routes first, then optional product APIs.
///
/// Installs connection-tracking middleware on the merged router so HTTP handlers
/// do not need to call [`TrembitaGatewayState::track_connection`] manually.
///
/// # Errors
/// [`GatewayConfigError`] when product APIs or `protect_apis` require identity.
pub fn build_gateway_router(
    app: &Arc<TrembitaApp>,
    config: GatewayConfig,
) -> Result<Router, GatewayConfigError> {
    validate_gateway_config(&config)?;
    let connections = app.cluster().workload_runtime().map_or_else(
        || Arc::new(ConnectionTracker::default()),
        |w| w.connections(),
    );
    build_gateway_router_with_tracker(app, config, Some(connections))
}

pub(super) fn build_gateway_router_with_tracker(
    app: &Arc<TrembitaApp>,
    config: GatewayConfig,
    connections: Option<Arc<ConnectionTracker>>,
) -> Result<Router, GatewayConfigError> {
    let GatewayConfig {
        addr: _,
        jobs_api,
        actors_api,
        workflows_api,
        introspect_api,
        identity,
        routes,
        drain_timeout: _,
        tls: _,
        protect_apis,
        rate_limit_per_sec,
    } = config;

    let needs_auth = protect_apis || jobs_api || actors_api || workflows_api || introspect_api;
    let auth = if needs_auth {
        identity.clone().map(identity_auth_fn)
    } else {
        None
    };

    let state = TrembitaGatewayState::from_parts(Arc::clone(app), identity, connections.clone());
    let mut router = routes.map_or_else(Router::new, |f| f(state));

    #[cfg(feature = "http-jobs")]
    {
        if workflows_api {
            let api = TrembitaApp::workflows_api(Arc::clone(app));
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }

        if actors_api {
            let api = TrembitaApp::actors_api(Arc::clone(app));
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }

        if jobs_api {
            let api = TrembitaApp::jobs_api(Arc::clone(app));
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }

        if introspect_api {
            let api = trembita_http::IntrospectApi::new(app.introspect_observer());
            router = router.merge(
                api.router()
                    .with_state(Arc::new(api.into_state_with_auth(auth.clone()))),
            );
        }
    }
    #[cfg(not(feature = "http-jobs"))]
    {
        let _ = (jobs_api, actors_api, workflows_api, introspect_api, app);
    }

    if let Some(connections) = connections {
        router = router.layer(axum::middleware::from_fn_with_state(
            connections,
            drain::track_connection,
        ));
    }

    if let Some(limit) = rate_limit_per_sec {
        router = router.layer(axum::middleware::from_fn_with_state(
            Arc::new(rate_limit::GatewayRateLimiter::new(limit)),
            rate_limit::rate_limit_middleware,
        ));
    }

    router = router.layer(DefaultBodyLimit::max(GATEWAY_MAX_BODY_BYTES));

    Ok(router)
}

#[cfg(feature = "http-jobs")]
pub(crate) fn identity_auth_fn(
    extractor: Arc<dyn identity::DynGatewayIdentity>,
) -> trembita_http::AuthFn {
    Arc::new(move |method, uri, headers| {
        let extractor = Arc::clone(&extractor);
        Box::pin(async move {
            let req = GatewayRequest::from_parts(&method, &uri, &headers);
            extractor
                .extract_dyn(&req)
                .await
                .map_err(|e| trembita_http::JobsApiError::Unauthorized(e.to_string()))?;
            Ok(())
        })
    })
}
