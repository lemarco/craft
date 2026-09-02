# trembita-dashboard

Live observability dashboard and admin HTTP endpoints for
[trembita](https://crates.io/crates/trembita).

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

Enable via `TrembitaCluster::builder(...).admin_addr(...)` or run
[`trembita-node`](../trembita-node) from the repository with `TREMBITA_ADMIN` set.

## Documentation

- [docs.rs/trembita-dashboard](https://docs.rs/trembita-dashboard)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
