//! Custom HTTP surfaces — wired via `.gateway_routes()` / `GatewayOpts::surfaces()` in `app.rs`.
//!
//! Built-in tables: `trembita add ops-routes`, `trembita add jobs-routes`.
//! Custom hosts: `trembita add http-surface api --hosts api.example.com`.
//!
//! Each module exports `route_table() -> RouteTable`.

pub mod jobs;
pub mod ops;
