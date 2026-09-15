# trembita-http

Product HTTP helpers for [trembita](https://crates.io/crates/trembita) apps (0.4.0 native routing).

**Product apps:** depend on `trembita` with feature `http-jobs` (enabled by default) — types re-exported at the crate root and in `trembita::gateway::http`. See [facade ADR](../../docs/decisions/facade.md). Direct `trembita-http` dependency is for advanced/workspace use.

## Job enqueue API

Mount via [`RouteTable`](src/routing/table.rs):

```rust
use trembita_http::{JobsApi, RouteTable};

let api = JobsApi::new(/* enqueue closure from TrembitaApp */);
let routes = api.route_table();
```

### `POST /jobs/{stream}`

Accepts work and returns **`202 Accepted`** with `{ "job_id": <u64> }`.

| Request body | Interpretation |
|--------------|----------------|
| Raw bytes (`application/octet-stream` or other) | Opaque job payload |
| JSON `{"payload":"..."}` | UTF-8 string as bytes |
| JSON `{"payload_b64":"..."}` | Base64-decoded bytes |

Optional query: `?priority=N`, `?dedup=KEY`.

### `POST /jobs/{stream}/batch`

Enqueue up to 256 jobs in one leader transaction. JSON body:

```json
{ "jobs": [{ "payload": "…", "priority": 1, "dedup": "key" }] }
```

Returns **`202 Accepted`** with `{ "job_ids": […] }`.

### `POST /jobs/{stream}/ack-batch`

Acknowledge many leases in one transaction (HTTP workers). JSON body:

```json
{ "worker_node": 1, "worker_instance": 0, "lease_ids": [100, 101] }
```

Returns **`200 OK`** with `{ "acked": N }`.

### `POST /actors/{group}/ask`

Request/reply to a worker group (round-robin). Same body rules as job enqueue (raw or JSON `payload` / `payload_b64`).

Returns **`200 OK`** with `{ "reply_b64": "…" }`, or raw bytes when `Accept: application/octet-stream`.

### `POST /actors/{group}/cast`

Fire-and-forget message to a worker group. Same body rules as above.

Returns **`202 Accepted`**.

## Introspect API

Read-only cluster snapshots (same JSON as ops `/introspect/*` on `TREMBITA_LISTEN`):

```rust
use std::sync::Arc;
use trembita_http::{IntrospectApi, Observer, RouteTable};

let observer: Arc<dyn Observer> = app.introspect_observer();
let api = IntrospectApi::new(observer);
let routes = api.route_table_with_auth(Some(auth_fn));
```

Or merge built-in tables in gateway surfaces (0.5+ — `with_*_api` removed):

```rust
GatewayOpts::new(addr)
    .identity(MySessionIdentity)
    .surfaces(|state| {
        let jobs = TrembitaApp::jobs_api(Arc::clone(&state.app))
            .route_table()
            .with_auth_mode(AuthMode::Identity);
        let introspect = state.app.introspect_api().route_table_with_auth(Some(auth_fn));
        custom_routes(state).merge(jobs).merge(introspect)
    })
```

Routes: `GET /introspect/cluster`, `/actors`, `/queues`, `/sagas`, `/raft-groups`, `/actors/{id}`, `/node/{id}`.

## Gateway surfaces (`Gateway` + `Surface`)

Several hostnames on one port — strict by default (unknown host → **404**):

```rust
use http::StatusCode;
use trembita_http::{CorsPolicy, Gateway, RequestCtx, Response, RouteTable, Surface};

let gateway = Gateway::new(false)
    .surface(|s| {
        s.hosts(["api.example.com"])
            .cors(CorsPolicy::allow_origins(["https://app.example.com"]))
            .routes(
                RouteTable::new().get("/health", |_: RequestCtx| async {
                    Ok(Response::status(StatusCode::OK))
                }),
            )
    })
    .dev_fallback(
        RouteTable::new().get("/health", |_: RequestCtx| async {
            Ok(Response::status(StatusCode::OK))
        }),
    );

let service = gateway.build_service()?;
```

Register every production hostname explicitly. Loopback-only routes go in [`Gateway::dev_fallback`](src/gateway/mod.rs) (omitted when `is_production` is true).

### Session and identity gates

- [`SessionGate`](src/routing/auth.rs) on a surface — use `.post_session("/path", …)` for cookie-protected routes.
- Gateway identity — use `.get_identity("/path", …)` plus `GatewayService::with_identity` (wired automatically when product APIs require auth).

## Static sites (`StaticSite`)

Serve product SPAs from one of three backends — same route table shape, switch via config:

| Backend | Use case |
|---------|----------|
| [`StaticSource::Embedded`](src/static_site/mod.rs) | Release binary (`include_dir!("../fe/dist")`) |
| [`StaticSource::Filesystem`](src/static_site/mod.rs) | Dev/staging (`TREMBITA_STATIC_CLIENT_ROOT=/path/to/dist`) |
| [`StaticSource::ObjectStore`](src/static_site/object_store.rs) | S3/MinIO/R2 (feature `static-s3`) |

```rust
use trembita_http::{Gateway, RouteTable, StaticSite, StaticSource, embedded_from_dir};

static CLIENT: include_dir::Dir<'_> = include_dir!("../fe/client/dist");

let site = StaticSite::new(StaticSource::embedded(embedded_from_dir(&CLIENT)))
    .spa_fallback(true);

let gateway = Gateway::new(false)
    .surface(|s| {
        s.hosts(["app.example.com"])
            .routes(site.route_table())
    })
    .surface(|s| s.hosts(["api.example.com"]).routes(api_routes));
```

Runtime env (filesystem example):

```bash
export TREMBITA_STATIC_CLIENT_SOURCE=filesystem
export TREMBITA_STATIC_CLIENT_ROOT=/var/www/client/dist
```

S3 example:

```bash
export TREMBITA_STATIC_ADMIN_SOURCE=s3
export TREMBITA_STATIC_ADMIN_BUCKET=my-app-fe-admin
export TREMBITA_STATIC_ADMIN_PREFIX=releases/latest/
export AWS_ACCESS_KEY_ID=…
export AWS_SECRET_ACCESS_KEY=…
```

See [gateway-routing-v2](../../docs/decisions/gateway-routing-v2.md) for the full 0.4.0 migration guide.
