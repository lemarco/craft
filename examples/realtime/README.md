# Real-time sessions (tier B)

WebSocket gateway → sticky [`ActorSession`](../../crates/crafty-actor/src/session.rs) → in-memory chat workers.

## What you run

| Piece | Role |
|-------|------|
| This binary | WS gateway + `ChatWorker` actors |
| [`trigger.sh`](trigger.sh) | One WS message via `crafty-showcase-client` or [websocat](https://github.com/vi/websocat) |
| [`trigger-batch.sh`](trigger-batch.sh) | Multi-user chat burst |
| Admin | Dashboard + actor directory |

## Quick start (local — one terminal)

**Terminal 1:**

```bash
cd examples/realtime
cargo run --release
```

**Terminal 2** (requires websocat):

```bash
./trigger.sh alice hello
./trigger-batch.sh 6
```

Manual connect:

```bash
websocat 'ws://127.0.0.1:8294/ws?user=alice'
```

## Quick start (cluster — 3 terminals, QUIC)

Three **identical** nodes — each runs WS gateway + chat workers:

```bash
cd examples/realtime
./cluster.sh setup
./cluster.sh up
./cluster.sh health
./trigger-batch.sh 9
```

| Node | QUIC | Admin | Gateway |
|------|------|-------|---------|
| 1 | `:7743` | `:9380` | `:8294` WS |
| 2 | `:7753` | `:9381` | `:8295` WS |
| 3 | `:7763` | `:9382` | `:8296` WS |

Connect to any node's WebSocket URL; sessions stick to a worker instance cluster-wide. Server logs show `[chat node N]` as messages land. Forward **8294** (or 8295/8296) and **9380** in Cursor/SSH.

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `CRAFTY_GATEWAY` | `127.0.0.1:8294` | WS bind (`-` disables gateway on a node) |
| `CRAFTY_PEERS` | unset | When set → QUIC cluster mode |
| `GATEWAY_TOKEN` | unset | Optional `?token=` on connect |
| `CRAFTY_DATA_DIR` | `/tmp/crafty-showcase-realtime` | Cluster + actor data |

Guide: [docs/scenarios/realtime-sessions.md](../../docs/scenarios/realtime-sessions.md)
