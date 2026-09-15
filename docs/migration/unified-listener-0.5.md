# Migrating to trembita 0.5.0 — unified HTTP listener

**From:** separate admin / gateway env vars, hidden `.with_*_api()` merges, `TrembitaClusterBuilder::admin_addr`  
**To:** one published port (`TREMBITA_LISTEN`) with default gateway surfaces or explicit [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) merges on that TCP bind

See [unified-listener](../decisions/unified-listener.md) for rationale.

## Who is affected

| Deployment | Action |
|------------|--------|
| [`TrembitaApp`](../../crates/trembita/src/app/mod.rs) product apps | Merge ops/jobs/workflows/actors route tables in `GatewayOpts::surfaces()` |
| [`trembita-node`](../../crates/trembita-tools/src/bin/node.rs) | Replace `TREMBITA_ADMIN` with ops TCP on **`TREMBITA_LISTEN`**; [`spawn_cluster_ops_http`](../../crates/trembita/src/gateway/cluster_ops.rs) when not using `TrembitaApp` |
| [`TrembitaCluster`](../../crates/trembita/src/cluster_handle/cluster.rs) without app gateway | Use [`cluster_ops_route_table`](../../crates/trembita/src/gateway/cluster_ops.rs) / `spawn_cluster_ops_http` |
| Showcases / compose | One published port per node (product + `/health`, `/dashboard`, …) |

## Environment variables

Product deploys use **four variables** (+ optional auth): see **[env.md](../env.md)**.

| Removed / deprecated | Replacement |
|---------------------|-------------|
| `TREMBITA_ADMIN`, separate gateway/admin TCP ports | **`TREMBITA_LISTEN` only** (same port number for UDP + TCP) |
| `TREMBITA_GATEWAY_JOBS`, `TREMBITA_GATEWAY_WORKFLOWS`, … | App registration / [`TrembitaApp::from_env`](../crates/trembita/src/app/runtime.rs) default gateway surfaces |
| `TREMBITA_HTTP=0.0.0.0:8090` while wire is `:7543` | **Rejected** — one `host:port` for wire and HTTP |
| `TREMBITA_NODE_ID`, static `TREMBITA_PEERS` on app compose | Dynamic join + `TREMBITA_DATA_DIR/node-id` |
| Per-file `TREMBITA_NODE_CERT` / `KEY` / `CA_CERT` | `TREMBITA_CERT_DIR` + `node-{id}.pem` |

| Variable | Role |
|----------|------|
| `TREMBITA_LISTEN` | **Single port config:** QUIC (UDP) + product/ops HTTP (TCP) (default `0.0.0.0:443`) |
| `TREMBITA_HTTP` / `TREMBITA_GATEWAY` | **Internal:** `-` disables TCP only; otherwise omit |
| `TREMBITA_HTTP_TLS_CERT` / `TREMBITA_HTTP_TLS_KEY` | Server TLS for TCP listener (advanced) |

Probes and dashboards: `GET /health`, `GET /ready`, `GET /metrics` on the **same** host/port as product APIs (often a dedicated ops hostname in production).

## Product apps (`TrembitaApp`)

### Removed APIs (0.5.0)

- `GatewayOpts::with_jobs_api`, `with_actors_api`, `with_workflows_api`, `with_introspect_api`
- `GatewayOpts::protect_product_apis`
- Hidden `collect_builtin_routes()` merges

### Explicit route tables

Scaffold layout (see [framework-conventions](../decisions/framework-conventions.md)):

```text
src/http/
├── mod.rs
├── ops.rs    # /health, /ready, /metrics, /dashboard, /introspect/*
├── jobs.rs   # /jobs/* (optional)
└── …         # custom surfaces (manual modules + gateway `.surface()`)
```

Default (local dev — ops + registered product APIs on loopback):

```rust
TrembitaApp::from_env()?
    .jobs([JobOpts::new("jobs", Duration::from_secs(60)).http_enqueue(true)])
    .gateway_routes(|state| http::product::route_table(&state))
    .run(RunOpts::from_env()?)
    .await?;
```

Host split (production):

```rust
.gateway(
    GatewayOpts::from_env()?.surfaces(|state| {
        let apis = DefaultGatewayApis { jobs: true, ..DefaultGatewayApis::default() };
        let product = TrembitaApp::default_product_routes(&state, apis)
            .merge(http::product::route_table(&state));
        let ops = state.app.ops_api().route_table();
        Gateway::new(true)
            .surface_hosts(["api.example.com"], product)
            .surface_hosts(["ops.internal"], ops)
    }),
)
```

Brownfield apps:

```bash
# Copy scaffold src/http/ops.rs and jobs.rs (or from examples/) and wire merges in app.rs
trembita doctor   # flags leftover with_*_api / protect_product_apis / TREMBITA_ADMIN in src/
trembita doctor --preflight   # before deploy: env, compose, certs, default ops on LISTEN
```

Ops table from runtime:

```rust
pub fn route_table(state: &TrembitaGatewayState) -> RouteTable {
    state.app.ops_api().route_table()
}
```

Jobs (identity-protected when gateway identity is configured):

```rust
TrembitaApp::jobs_api(Arc::clone(&state.app))
    .route_table()
    .with_auth_mode(AuthMode::Identity)
```

See [getting-started.md](../getting-started.md) env table and [examples/](../../examples/README.md) showcases (unified HTTP on one port).

## Auth migration (from `protect_product_apis`)

| 0.4.x | 0.5.0 |
|-------|-------|
| `protect_product_apis(true)` on `GatewayOpts` | `RouteTable::with_auth_mode(AuthMode::Identity)` on each built-in API table |
| Per-handler `authorize()` on some paths | Route-level auth at dispatch ([gateway-routing-v2](../decisions/gateway-routing-v2.md)) |

## Cluster-only nodes (`TrembitaCluster`)

Removed:

- `TrembitaClusterBuilder::admin_addr` / `admin_tls`

Added:

- [`cluster_ops_route_table`](../../crates/trembita/src/gateway/cluster_ops.rs) — ops routes for a cluster handle
- [`spawn_cluster_ops_http`](../../crates/trembita/src/gateway/cluster_ops.rs) — TCP listener (used by `trembita-node` when `TREMBITA_HTTP` is set)

`TrembitaApp::ops_api()` remains the same route builders; only **where** you mount them changed (explicit merge or cluster helper).

## `trembita-node`

- Ops HTTP: TCP on the **same `host:port` as `TREMBITA_LISTEN`** ([`product_http_from_wire`](../../crates/trembita/src/env_config.rs)); optional **`TREMBITA_HTTP_TLS_*`**
- Set **`TREMBITA_HTTP=-`** only for QUIC-only nodes (no TCP listener)
- No separate admin listener

## Docker / k8s

Each process has **two** socket binds on one port number — not split admin/gateway TCP ports:

| Bind | Env | Protocol | Purpose |
|------|-----|----------|---------|
| Wire | `TREMBITA_LISTEN` | QUIC (UDP) | Raft / actor traffic between nodes |
| HTTP | `TREMBITA_LISTEN` (same port number) | TCP | Product routes **and** ops on **one** listener |

Do **not** set `TREMBITA_HTTP` / `TREMBITA_GATEWAY` to a different host:port than `TREMBITA_LISTEN`. Remove legacy admin-only port mappings (`9180`, `9280`, …).

- One Service port for HTTP (product + ops), or split by **hostname** on the same TCP bind via `Gateway::surface(hosts: …)`.
- Publish only the HTTP port to clients; QUIC stays on the mesh network.

## Checklist

- [ ] Replace `TREMBITA_ADMIN` / split ports with **`TREMBITA_LISTEN`** (wire + TCP)
- [ ] Merge `http::ops::route_table` (and product APIs) in `.surfaces()`
- [ ] Run `trembita doctor`; fix errors for removed gateway flags
- [ ] Point monitoring at `/health` and `/metrics` on the unified bind
- [ ] Read [gateway-0.4.md](gateway-0.4.md) if still on Axum / `.routes()` from 0.3.x

## Related

- [CHANGELOG 0.5.0](../../CHANGELOG.md)
- [introspect-api](../decisions/introspect-api.md) — introspection as mountable routes
- [production-runbook.md](../ops/production-runbook.md) — ops merge on private bind
