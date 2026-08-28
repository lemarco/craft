# crafty-dashboard

Live observability dashboard and admin HTTP endpoints for
[crafty](https://crates.io/crates/crafty).

Serves health/readiness probes, Prometheus metrics, JSON introspection, and a
read-only web UI on the **admin port** (default `0.0.0.0:8080`), separate from
the mTLS QUIC cluster wire.

| Route | Purpose |
|-------|---------|
| `/health`, `/ready` | Liveness / readiness |
| `/metrics` | Prometheus text |
| `/introspect/*` | Cluster and actor snapshots |
| `/dashboard` | Live HTML dashboard |
| `/dashboard/events` | SSE event feed |

Enable via `CraftyCluster::builder(...).admin_addr(...)` or run
[`crafty-node`](https://crates.io/crates/crafty-node) with `CRAFTY_ADMIN` set.

## Documentation

- [docs.rs/crafty-dashboard](https://docs.rs/crafty-dashboard)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
