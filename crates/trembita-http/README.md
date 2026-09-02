# trembita-http

Product HTTP helpers for [trembita](https://crates.io/crates/trembita) apps.

## Job enqueue API

Mount on any Axum router:

```rust
use std::sync::Arc;
use trembita_http::{JobsApi, JobsApiState};

let api = JobsApi::new(/* enqueue closure from TrembitaApp */);
let app = axum::Router::new()
    .merge(api.router())
    .with_state(Arc::new(api.into_state()));
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

## Virtual hosts (`HostRouter`)

Several hostnames on one port — strict by default (unknown host → **404**):

```rust
use trembita_http::HostRouter;

let api = axum::Router::new().route("/health", get(|| async { "ok" }));
let ws = axum::Router::new().route("/ws", get(ws_upgrade));

let app = HostRouter::new()
    .host("api.example.com", api)
    .host("ws.example.com", ws)
    .local_dev_fallback(single_host_router_for_local_dev) // localhost only
    .build();
```

Do **not** use a catch-all dev router in production — register every production
hostname explicitly, or opt in to [`HostRouter::unmatched_fallback`] deliberately.
