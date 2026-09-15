# Stateful workers (stateful actors)

Idempotent order processing with [`ActorStateStore`](../../crates/trembita-actor-store/src/store.rs) and a two-node migration walkthrough.

## What you run

| Piece | Role |
|-------|------|
| This binary | `OrderProcessor` actor + HTTP cast API + authenticated submit route |
| [`trigger.sh`](trigger.sh) | Cast one order id via built-in `/actors/orders/cast` (`202 Accepted`) |
| [`trigger-auth.sh`](trigger-auth.sh) | Same flow via custom `POST /orders/submit` + [`GatewayIdentity`](../../crates/trembita/src/gateway/identity.rs) |
| [`trigger-batch.sh`](trigger-batch.sh) | Round-robin cast across gateways + idempotency re-send |
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

**Migration demo** (separate one-shot, LocalNetwork):

```bash
cargo run --release -- migrate-demo
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

Each listener accepts `POST /actors/orders/cast` and forwards to the supervisor-placed `orders` actor. Re-send the same order id from any node — second call is an idempotent skip (check server logs + `/dashboard`).

Forward **8190** (or 8191/8192) in Cursor/SSH for browser access.

## curl equivalent

Postcard-encoded `u64` order id:

```bash
# order 1001 = bytes e9 07 00 00 00 00 00 00 (little-endian u64)
curl -X POST http://127.0.0.1:8190/actors/orders/cast \
  -H 'content-type: application/octet-stream' \
  --data-binary $'\xe9\x07\x00\x00\x00\x00\x00\x00'
```

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `TREMBITA_LISTEN` | `127.0.0.1:8190` | One port — QUIC + HTTP product + ops |
| `TREMBITA_JOIN_SEEDS` | unset | Joiners: `1@127.0.0.1:8190` (see `./cluster.sh`) |
| `TREMBITA_DATA_DIR` | `/tmp/trembita-showcase-stateful-workers` | redb + assigned `node-id` per node |
| `TREMBITA_GATEWAYS` | `8190 8191 8192` | Round-robin hosts for `trigger-batch.sh` |
| `GATEWAY_TOKEN` | unset | When set, identity-protected routes require Bearer + `X-Trembita-User` |

Built-in [`ActorsApi`](../../crates/trembita/src/app/runtime.rs) and custom `/orders/submit` use
[`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs) on the route table (see `src/main.rs`).

Guide: [docs/scenarios/stateful-workers.md](../../docs/scenarios/stateful-workers.md)
