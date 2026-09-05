use std::sync::Arc;

use trembita_http::{Gateway, GatewayService, RouteTable};
use trembita_runtime::ComputeTokenPool;

use super::super::app::TrembitaApp;
use super::config::{GatewayConfig, GatewayConfigError, validate_gateway_config};
use super::drain::ConnectionTracker;
use super::identity::{self, GatewayBearerIdentity, GatewayRequest};
use super::rate_limit::GatewayRateLimiter;
use super::state::TrembitaGatewayState;

/// Bearer auth hook for product/upgrade HTTP when a gateway token env var is set.
#[must_use]
pub fn bearer_auth_from_env() -> Option<trembita_http::AuthFn> {
    super::config::gateway_token_from_env()
        .map(|_| identity_auth_fn(identity::erase_identity(GatewayBearerIdentity::from_env())))
}

/// Build the gateway hyper service: user surfaces + optional product APIs.
///
/// # Errors
/// [`GatewayConfigError`] when product APIs or `protect_apis` require identity.
pub fn build_gateway_service(
    app: &Arc<TrembitaApp>,
    config: GatewayConfig,
) -> Result<WrappedGatewayService, GatewayConfigError> {
    validate_gateway_config(&config)?;
    let connections = app.cluster().workload_runtime().map_or_else(
        || Arc::new(ConnectionTracker::default()),
        |w| w.connections(),
    );
    build_gateway_service_with_tracker(app, config, Some(connections))
}

#[allow(clippy::unnecessary_wraps)]
pub(super) fn build_gateway_service_with_tracker(
    app: &Arc<TrembitaApp>,
    config: GatewayConfig,
    connections: Option<Arc<ConnectionTracker>>,
) -> Result<WrappedGatewayService, GatewayConfigError> {
    let GatewayConfig {
        addr: _,
        jobs_api,
        actors_api,
        workflows_api,
        introspect_api,
        identity,
        surfaces,
        drain_timeout: _,
        tls: _,
        protect_apis,
        rate_limit_per_sec,
    } = config;

    let identity_hook = identity.clone().map(identity_auth_fn);
    let product_auth = if protect_apis || jobs_api || actors_api || workflows_api || introspect_api
    {
        identity_hook.clone()
    } else {
        None
    };

    let state = TrembitaGatewayState::from_parts(Arc::clone(app), identity, connections.clone());
    let mut gateway = surfaces.map_or_else(|| Gateway::new(false), |f| f(state));
    gateway = gateway.merge_routes(collect_builtin_routes(
        app,
        product_auth,
        jobs_api,
        actors_api,
        workflows_api,
        introspect_api,
    ));

    let inner = {
        let mut svc = GatewayService::build(&gateway)?;
        if let Some(hook) = identity_hook {
            svc = svc.with_identity(trembita_http::auth_fn_to_identity(hook));
        }
        svc
    };

    let compute_pool = app.cluster().workload_runtime().map(|w| w.pool());

    Ok(WrappedGatewayService {
        inner,
        connections,
        rate_limiter: rate_limit_per_sec.map(GatewayRateLimiter::new),
        compute_pool,
    })
}

fn collect_builtin_routes(
    app: &Arc<TrembitaApp>,
    auth: Option<trembita_http::AuthFn>,
    jobs_api: bool,
    actors_api: bool,
    workflows_api: bool,
    introspect_api: bool,
) -> RouteTable {
    let mut table = RouteTable::new();
    #[cfg(feature = "http-jobs")]
    {
        if workflows_api {
            let api = TrembitaApp::workflows_api(Arc::clone(app));
            table = table.merge(api.route_table_with_auth(auth.clone()));
        }
        if actors_api {
            let api = TrembitaApp::actors_api(Arc::clone(app));
            table = table.merge(api.route_table_with_auth(auth.clone()));
        }
        if jobs_api {
            let api = TrembitaApp::jobs_api(Arc::clone(app));
            table = table.merge(api.route_table_with_auth(auth.clone()));
        }
        if introspect_api {
            let api = trembita_http::IntrospectApi::new(app.introspect_observer());
            table = table.merge(api.route_table_with_auth(auth.clone()));
        }
    }
    #[cfg(not(feature = "http-jobs"))]
    {
        let _ = (
            app,
            auth,
            jobs_api,
            actors_api,
            workflows_api,
            introspect_api,
        );
    }
    table
}

/// Gateway service with connection tracking, rate limiting, and compute tokens.
#[derive(Clone)]
pub struct WrappedGatewayService {
    inner: GatewayService,
    connections: Option<Arc<ConnectionTracker>>,
    rate_limiter: Option<GatewayRateLimiter>,
    compute_pool: Option<Arc<ComputeTokenPool>>,
}

impl WrappedGatewayService {
    /// Underlying dispatch service (tests, route merging).
    #[must_use]
    pub fn inner(&self) -> &GatewayService {
        &self.inner
    }
}

impl tower::Service<http::Request<hyper::body::Incoming>> for WrappedGatewayService {
    type Response = http::Response<
        http_body_util::combinators::BoxBody<bytes::Bytes, std::convert::Infallible>,
    >;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<hyper::body::Incoming>) -> Self::Future {
        if let Some(limiter) = &self.rate_limiter
            && !limiter.try_acquire()
        {
            return Box::pin(async move { Ok(too_many_requests()) });
        }

        let _guard = self.connections.as_ref().map(|c| c.track());
        let compute = self.compute_pool.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let _compute = if let Some(pool) = compute {
                Some(pool.acquire().await)
            } else {
                None
            };
            inner.call(req).await
        })
    }
}

fn too_many_requests()
-> http::Response<http_body_util::combinators::BoxBody<bytes::Bytes, std::convert::Infallible>> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    let body = Full::new(Bytes::from_static(b"rate limit exceeded"));
    let mut resp = http::Response::new(body.map_err(|never| match never {}).boxed());
    *resp.status_mut() = http::StatusCode::TOO_MANY_REQUESTS;
    resp
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
