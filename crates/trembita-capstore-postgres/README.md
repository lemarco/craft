# trembita-capstore-postgres

Optional PostgreSQL backend for [`CapStateStore`](https://docs.rs/trembita-capstore/latest/trembita_capstore/trait.CapStateStore.html) — idempotency keys, handler markers, and workflow progress outside embedded redb.

## Schema

Default table `trembita_capstore_kv`:

| column | type |
|--------|------|
| `key` | `TEXT PRIMARY KEY` |
| `value` | `BYTEA NOT NULL` |
| `expires_at_ms` | `BIGINT` (nullable, UTC millis) |

Create it once per database (see `PgCapStore::ddl()`).

## Usage

```rust
use std::sync::Arc;
use trembita_capstore::{CapStateStore, StoreError};
use trembita_capstore_postgres::PgCapStore;

# async fn demo() -> Result<(), StoreError> {
let store = PgCapStore::connect("postgres://localhost/trembita", "trembita_capstore_kv").await?;
let store: Arc<dyn CapStateStore> = Arc::new(store);
store.set("order:42", b"processing", None).await?;
# Ok(())
# }
```

Pair with the facade via feature `capstore-postgres` on `trembita` when wiring custom boot code.

[`compare_and_set`](https://docs.rs/trembita-capstore/latest/trembita_capstore/trait.CapStateStore.html#tymethod.compare_and_set) is implemented with conditional SQL (`INSERT … ON CONFLICT` / `UPDATE … RETURNING`) so concurrent workers sharing a pool get the same semantics as Redis Lua CAS.
