//! Custom HTTP surfaces — wired via `GatewayOpts::surfaces()` in `app.rs`.
//!
//! Add surfaces with:
//! ```bash
//! trembita add http-surface api --hosts api.example.com
//! ```
//!
//! Each module exports `route_table() -> RouteTable`.
