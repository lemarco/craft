# trembita-capstore

Durable **capability workflow keys** for trembita — idempotency markers, handler progress, and optimistic concurrency via [`CapStateStore`](https://docs.rs/trembita-capstore/latest/trembita_capstore/trait.CapStateStore.html).

Production default: embedded **`RedbCapStateStore`** under `{data_dir}/actor-store.redb` (filename unchanged for on-disk compatibility). Product API: [`trembita::capstore`](https://docs.rs/trembita/latest/trembita/capstore/index.html).

Optional backends:

| Crate | Backend |
|-------|---------|
| [`trembita-store-redis`](https://crates.io/crates/trembita-store-redis) | Redis |
| [`trembita-capstore-postgres`](https://crates.io/crates/trembita-capstore-postgres) | PostgreSQL |

The crates.io name **`trembita-actor-store`** remains a deprecated re-export shim (0.6.1+).
