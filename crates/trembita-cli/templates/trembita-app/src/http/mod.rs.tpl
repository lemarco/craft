//! Custom HTTP surfaces — wired via `.gateway_routes()` / `GatewayOpts::surfaces()` in `app.rs`.
//!
//! Built-in tables: copy/adapt `ops.rs` / `jobs.rs` from the scaffold template.
//! Custom hosts: add modules here and `.surface()` in `app.rs`.
//!
//! Each module exports `route_table() -> RouteTable`.

pub mod jobs;
pub mod ops;
