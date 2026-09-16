# Stateful workers (capabilities + migration demo)

Default mode registers the **`orders`** capability ([`src/capabilities/orders/`](src/capabilities/orders/mod.rs)) with idempotent [`ActorStateStore`](../../crates/trembita-actor-store/src/store.rs) keys — **product migration** is per-op store keys, not snapshotting actor RAM ([capability-greenfield-wire](../../docs/decisions/capability-greenfield-wire.md)).

**Advanced only:** `migrate-demo` / `TREMBITA_MIGRATE_DEMO=1` runs a separate [`UserActor`](../../crates/trembita-runtime/src/registry/actor.rs) counter with in-memory RAM migration — for runtime education, not the default app path.

## What you run

| Piece | Role |
|-------|------|
| This binary | `CapManifest` (`orders.process`) + authenticated `POST /orders/submit` (capability fire) |
| [`trigger.sh`](trigger.sh) | Submit one order id via `/orders/submit` (`202 Accepted`) |
| [`trigger-auth.sh`](trigger-auth.sh) | Same route with explicit tenant query + optional `GATEWAY_TOKEN` |
| [`trigger-batch.sh`](trigger-batch.sh) | Round-robin submit across gateways + idempotency re-send |
| Ops (same listener) | `/health`, `/metrics`, `/dashboard`, `/introspect/*` via merged [`OpsApi`](../../crates/trembita-http/src/ops_routes.rs) |

## Quick start (local — one terminal)

**Terminal 1:**

```bash
cd examples/stateful-workers
cargo run --release
```

**Terminal 2:**

```bash
./trigger.sh 1001
./trigger-auth.sh tenant-1 1001   # custom gateway route + sticky session key
./trigger.sh 1001   # second call — idempotent skip
```

**Advanced migration demo** (in-memory `UserActor`, separate from orders capability):

```bash
cargo run --release -- migrate-demo
# QUIC: TREMBITA_MIGRATE_DEMO=1 ./cluster.sh 1-migrate …
```

## Quick start (cluster — 3 terminals, QUIC)

```bash
cd examples/stateful-workers
./cluster.sh setup

./cluster.sh 1   # HTTP :8190 (product + ops)
./cluster.sh 2   # :8191
./cluster.sh 3   # :8192

./cluster.sh health
./trigger-batch.sh 10
```

| Node | `TREMBITA_LISTEN` (wire + HTTP) |
|------|----------------------------------|
| 1 | `:8190` |
| 2 | `:8191` |
| 3 | `:8192` |

Each listener accepts `POST /orders/submit` and fires the `orders.process` capability. Re-send the same order id from any node — second call is an idempotent skip (check server logs + `/dashboard`).

Forward **8190** (or 8191/8192) in Cursor/SSH for browser access.

## curl equivalent

```bash
curl -X POST 'http://127.0.0.1:8190/orders/submit?user=dev&token=showcase' \
  -H 'content-type: application/json' \
  -d '{"order_id":1001}'
```

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `TREMBITA_LISTEN` | `127.0.0.1:8190` | One port — QUIC + HTTP product + ops |
| `TREMBITA_JOIN_SEEDS` | unset | Joiners: `1@127.0.0.1:8190` (see `./cluster.sh`) |
| `TREMBITA_DATA_DIR` | `/tmp/trembita-showcase-stateful-workers` | redb + assigned `node-id` per node |
| `TREMBITA_GATEWAYS` | `8190 8191 8192` | Round-robin hosts for `trigger-batch.sh` |
