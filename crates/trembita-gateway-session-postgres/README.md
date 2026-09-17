# trembita-gateway-session-postgres

Optional PostgreSQL registry for **opaque** gateway session cookies (B-46) — dedicated table, separate from [`trembita-capstore-postgres`](../trembita-capstore-postgres/) KV rows used for idempotency markers.

## Schema

Default table `trembita_gateway_sessions` — see [`PgGatewaySessionStore::ddl`](src/lib.rs).

## Usage

```rust
use std::sync::Arc;
use trembita_gateway_session_postgres::PgGatewaySessionStore;

# async fn demo() -> Result<(), trembita_capstore::StoreError> {
let store = PgGatewaySessionStore::connect("postgres://localhost/trembita", "trembita_gateway_sessions").await?;
# Ok(())
# }
```

Implement [`GatewaySessionStore`](https://docs.rs/trembita/latest/trembita/trait.GatewaySessionStore.html) and wire [`CapStoreSessionVerifier`](https://docs.rs/trembita/latest/trembita/struct.CapStoreSessionVerifier.html) via **`trembita`** feature **`gateway-session-postgres`**:

```rust
use std::sync::Arc;
use trembita::{pg_gateway_session_gate, CapStoreSessionIssuer, GatewaySessionStore, PgGatewaySessionStore};
use trembita_http::CookieConfig;

let store = Arc::new(PgGatewaySessionStore::connect(url, "trembita_gateway_sessions").await?);
let gate = pg_gateway_session_gate(Arc::clone(&store), CookieConfig::default());
let issuer = CapStoreSessionIssuer::new(store as Arc<dyn GatewaySessionStore>);
```

Integration tests: `cargo test -p trembita-gateway-session-postgres --features docker-tests -- --ignored`.
