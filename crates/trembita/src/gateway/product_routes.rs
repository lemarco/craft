//! Fluent [`RouteTable`] builder for product HTTP handlers.

use trembita_http::{Handler, RouteTable};

/// Chain product routes without manual [`RouteTable::merge`] boilerplate.
#[derive(Debug, Default)]
pub struct ProductRoutes(RouteTable);

impl ProductRoutes {
    /// Empty table — chain [`.get`](Self::get), [`.post`](Self::post), … then [`.build`](Self::build).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open `GET` route.
    #[must_use]
    pub fn get(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.0 = self.0.get(path, handler);
        self
    }

    /// Open `POST` route.
    #[must_use]
    pub fn post(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.0 = self.0.post(path, handler);
        self
    }

    /// Identity-protected `GET` ([`AuthMode::Identity`](trembita_http::AuthMode::Identity)).
    #[must_use]
    pub fn get_identity(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.0 = self.0.get_identity(path, handler);
        self
    }

    /// Identity-protected `POST`.
    #[must_use]
    pub fn post_identity(mut self, path: &str, handler: impl Handler + 'static) -> Self {
        self.0 = self.0.post_identity(path, handler);
        self
    }

    /// Finish the table for [`.gateway_routes`](crate::TrembitaAppBuilder::gateway_routes).
    #[must_use]
    pub fn build(self) -> RouteTable {
        self.0
    }
}
