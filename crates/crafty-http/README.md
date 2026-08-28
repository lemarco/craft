# crafty-http

Product HTTP helpers for [crafty](https://crates.io/crates/crafty) apps.

## Job enqueue API

Mount on any Axum router:

```rust
use std::sync::Arc;
use crafty_http::{JobsApi, JobsApiState};

let api = JobsApi::new(/* enqueue closure from CraftyApp */);
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
