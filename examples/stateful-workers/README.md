# Stateful workers (stateful actors)

Idempotent order processing with [`ActorStateStore`](../../crates/crafty-actor/src/store.rs) and a two-node migration walkthrough.

## What you run

| Piece | Role |
|-------|------|
| This binary | `OrderProcessor` actor + HTTP cast API + authenticated submit route |
| [`trigger.sh`](trigger.sh) | Cast one order id via built-in `/actors/orders/cast` (`202 Accepted`) |
| [`trigger-auth.sh`](trigger-auth.sh) | Same flow via custom `POST /orders/submit` + [`GatewayIdentity`](../../crates/crafty/src/gateway/identity.rs) |
| [`trigger-batch.sh`](trigger-batch.sh) | Round-robin cast across gateways + idempotency re-send |
| Admin | Dashboard + actor introspection |

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

./cluster.sh 1   # gateway :8190 + admin :9280
./cluster.sh 2   # gateway :8191
./cluster.sh 3   # gateway :8192

./cluster.sh health
./trigger-batch.sh 10
```

| Node | QUIC | Admin | Gateway |
|------|------|-------|---------|
| 1 | `:7643` | `:9280` | `:8190` |
| 2 | `:7653` | `:9281` | `:8191` |
| 3 | `:7663` | `:9282` | `:8192` |

Each gateway accepts `POST /actors/orders/cast` and forwards to the supervisor-placed `orders` actor. Re-send the same order id from any gateway — second call is an idempotent skip (check server logs + dashboard).

Forward **8190** and **9280** in Cursor/SSH for browser access.

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
| `CRAFTY_GATEWAY` | `127.0.0.1:8190` (local) | Product HTTP bind (`-` disables) |
| `CRAFTY_PEERS` | unset | When set → QUIC cluster mode |
| `CRAFTY_DATA_DIR` | `/tmp/crafty-showcase-stateful-workers` | redb actor store per node |
| `CRAFTY_GATEWAYS` | `8190 8191 8192` | Round-robin list for `trigger-batch.sh` |

Guide: [docs/scenarios/stateful-workers.md](../../docs/scenarios/stateful-workers.md)
