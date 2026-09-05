//! Gateway and surface builders (trembita 0.4.0).

mod cors;
mod dispatch;

use crate::routing::{RouteTable, SessionGate};

pub use cors::CorsPolicy;
pub use dispatch::{GatewayDispatch, GatewayService, UpgradeStream, is_websocket_upgrade};

/// One logical product surface — hostnames, optional gates, and routes.
#[derive(Debug)]
pub struct Surface {
    hosts: Vec<String>,
    cors: Option<CorsPolicy>,
    session: Option<SessionGate>,
    routes: RouteTable,
}

impl Surface {
    /// Start building a surface. Chain [`.cors`](Self::cors), [`.session`](Self::session),
    /// [`.routes`](Self::routes), then pass to [`Gateway::surface`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            hosts: Vec::new(),
            cors: None,
            session: None,
            routes: RouteTable::new(),
        }
    }

    /// Hostnames that route to this surface (`api.example.com`, internal aliases, …).
    #[must_use]
    pub fn hosts(mut self, hostnames: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.hosts = hostnames.into_iter().map(Into::into).collect();
        self
    }

    /// CORS policy for browser clients on this surface.
    #[must_use]
    pub fn cors(mut self, policy: CorsPolicy) -> Self {
        self.cors = Some(policy);
        self
    }

    /// Session cookie gate for all routes on this surface unless overridden per route.
    #[must_use]
    pub fn session(mut self, gate: SessionGate) -> Self {
        self.session = Some(gate);
        self
    }

    /// Route table for this surface.
    #[must_use]
    pub fn routes(mut self, table: RouteTable) -> Self {
        self.routes = table;
        self
    }

    /// Registered hostnames.
    #[must_use]
    pub fn host_list(&self) -> &[String] {
        &self.hosts
    }

    /// CORS policy, if any.
    #[must_use]
    pub fn cors_policy(&self) -> Option<&CorsPolicy> {
        self.cors.as_ref()
    }

    /// Session gate, if any.
    #[must_use]
    pub fn session_gate(&self) -> Option<&SessionGate> {
        self.session.as_ref()
    }

    /// Routes mounted on this surface.
    #[must_use]
    pub fn route_table(&self) -> &RouteTable {
        &self.routes
    }

    /// Append routes onto this surface.
    #[must_use]
    pub fn merge_routes(mut self, routes: RouteTable) -> Self {
        self.routes = self.routes.merge(routes);
        self
    }
}

impl Default for Surface {
    fn default() -> Self {
        Self::new()
    }
}

/// Declarative multi-host gateway — replaces [`crate::HostRouter`] + [`crate::MultiHostBuilder`]
/// in 0.4.0.
#[derive(Debug, Default)]
pub struct Gateway {
    surfaces: Vec<Surface>,
    dev_fallback: Option<RouteTable>,
    is_production: bool,
}

impl Gateway {
    /// Start gateway assembly. Pass `true` in production to omit dev-only fallbacks.
    #[must_use]
    pub fn new(is_production: bool) -> Self {
        Self {
            surfaces: Vec::new(),
            dev_fallback: None,
            is_production,
        }
    }

    /// Register one surface. The closure receives a [`Surface`] builder.
    #[must_use]
    pub fn surface(mut self, f: impl FnOnce(Surface) -> Surface) -> Self {
        self.surfaces.push(f(Surface::new()));
        self
    }

    /// Loopback-only routes (`localhost`, `127.0.0.1`, `::1`) — omitted when `is_production`.
    #[must_use]
    pub fn dev_fallback(mut self, routes: RouteTable) -> Self {
        self.dev_fallback = Some(routes);
        self
    }

    /// Whether this build targets production (no dev fallback).
    #[must_use]
    pub fn is_production(&self) -> bool {
        self.is_production
    }

    /// Registered surfaces.
    #[must_use]
    pub fn surfaces(&self) -> &[Surface] {
        &self.surfaces
    }

    /// Dev fallback routes, if configured and not production.
    #[must_use]
    pub fn dev_fallback_routes(&self) -> Option<&RouteTable> {
        if self.is_production {
            None
        } else {
            self.dev_fallback.as_ref()
        }
    }

    /// Merge `routes` into every surface, or into dev fallback when no surfaces exist.
    #[must_use]
    pub fn merge_routes(mut self, routes: RouteTable) -> Self {
        if routes.is_empty() {
            return self;
        }
        if self.surfaces.is_empty() {
            self.dev_fallback = Some(match self.dev_fallback.take() {
                Some(existing) => existing.merge(routes),
                None => routes,
            });
            return self;
        }
        self.surfaces = self
            .surfaces
            .into_iter()
            .map(|surface| surface.merge_routes(routes.clone()))
            .collect();
        self
    }

    /// Validate wiring and build a hyper [`GatewayService`](dispatch::GatewayService).
    ///
    /// # Errors
    /// [`GatewayBuildError`] when surfaces are invalid.
    pub fn build_service(&self) -> Result<GatewayService, GatewayBuildError> {
        GatewayService::build(self)
    }

    /// Validate wiring.
    ///
    /// # Errors
    /// [`GatewayBuildError`] when surfaces are invalid.
    pub fn validate(&self) -> Result<(), GatewayBuildError> {
        for (index, surface) in self.surfaces.iter().enumerate() {
            if surface.host_list().is_empty() {
                return Err(GatewayBuildError::MissingHosts { surface: index });
            }
            if surface.route_table().is_empty() {
                return Err(GatewayBuildError::EmptyRoutes { surface: index });
            }
        }
        Ok(())
    }
}

/// Invalid gateway wiring detected at build time.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayBuildError {
    /// A surface was registered without hostnames.
    #[error("surface {surface} has no hostnames")]
    MissingHosts {
        /// Index into [`Gateway::surfaces`].
        surface: usize,
    },
    /// A surface has hostnames but no routes.
    #[error("surface {surface} has no routes")]
    EmptyRoutes {
        /// Index into [`Gateway::surfaces`].
        surface: usize,
    },
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::*;
    use crate::routing::{RequestCtx, Response};

    #[test]
    fn validate_requires_hosts_and_routes() {
        let bad = Gateway::new(false).surface(|s| s.routes(RouteTable::new()));
        assert_eq!(
            bad.validate().expect_err("hosts"),
            GatewayBuildError::MissingHosts { surface: 0 }
        );

        let ok = Gateway::new(false).surface(|s| {
            s.hosts(["api.example.com"]).routes(
                RouteTable::new().get("/health", |_: RequestCtx| async {
                    Ok(Response::status(StatusCode::OK))
                }),
            )
        });
        ok.validate().expect("valid");
    }

    #[test]
    fn dev_fallback_hidden_in_production() {
        let g =
            Gateway::new(true).dev_fallback(RouteTable::new().get("/x", |_: RequestCtx| async {
                Ok(Response::status(StatusCode::OK))
            }));
        assert!(g.dev_fallback_routes().is_none());
    }
}
