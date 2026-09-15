# trembita-dashboard

Live observability dashboard and ops HTTP endpoints for
[trembita](https://crates.io/crates/trembita).

Serves health/readiness probes, Prometheus metrics, JSON introspection, and a
read-only web UI on the **ops HTTP bind** — for product apps, TCP on the same
`host:port` as **`TREMBITA_LISTEN`** (separate socket from QUIC/mTLS wire).

| Route | Purpose |
|-------|---------|
| `/health`, `/ready` | Liveness / readiness |
| `/metrics` | Prometheus text |
| `/introspect/*` | Cluster and actor snapshots |
| `/dashboard` | Live HTML dashboard |
| `/dashboard/events` | SSE event feed |

Mount [`OpsApi::route_table()`](../trembita-http/src/ops_routes.rs) on your gateway,
use [`TrembitaApp::from_env()`](../trembita/src/app/runtime.rs) (default surfaces),
or run [`trembita-node`](../trembita-tools) with ops TCP enabled (default = co-host on
`TREMBITA_LISTEN`; `TREMBITA_HTTP=-` disables TCP only).

## Documentation

- [docs.rs/trembita-dashboard](https://docs.rs/trembita-dashboard)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
