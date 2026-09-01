# Real-time sessions (stateful actors)

WebSocket **and authenticated HTTP** on one gateway → sticky [`ActorSession`](../../crates/crafty-actor/src/session.rs) → in-memory chat workers.

## What you run

| Piece | Role |
|-------|------|
| This binary | WS + HTTP gateway + `ChatWorker` actors |
| [`trigger.sh`](trigger.sh) | One WS message via `crafty-showcase-client` or [websocat](https://github.com/vi/websocat) |
| [`trigger-http.sh`](trigger-http.sh) | One HTTP `POST /chat` (JSON body + query or Bearer auth) |
| [`trigger-batch.sh`](trigger-batch.sh) | Multi-user chat burst |
| Admin | Dashboard + actor directory |

## Quick start (local — one terminal)

**Terminal 1:**

```bash
cd examples/realtime
cargo run --release
```

**Terminal 2:**

```bash
./trigger.sh alice hello          # WebSocket
./trigger-http.sh alice hello     # HTTP POST /chat
curl 'http://127.0.0.1:8294/me?user=alice'   # GET /me (identity only)
./trigger-batch.sh 6
```

Manual WebSocket:

```bash
websocat 'ws://127.0.0.1:8294/ws?user=alice'
```

Manual HTTP (Bearer when `GATEWAY_TOKEN` is set):

```bash
curl -X POST 'http://127.0.0.1:8294/chat' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -H 'X-Crafty-User: alice' \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello"}'
```

## Gateway routes

| Route | Auth | Body |
|-------|------|------|
| `GET /ws` | `?user=` (+ `?token=` if `GATEWAY_TOKEN` set) | WebSocket upgrade |
| `POST /chat` | query or Bearer + `X-Crafty-User` | `{"message":"…"}` |
| `GET /me` | query or Bearer | returns `{"user":"…"}` |

Identity implementation: [`crafty-showcase-common::ShowcaseGatewayIdentity`](../../crates/crafty-showcase-common/src/gateway_auth.rs).

## Quick start (cluster — 3 terminals, QUIC)

Three **identical** nodes — each runs WS + HTTP gateway + chat workers:

```bash
cd examples/realtime
./cluster.sh setup
./cluster.sh up
./cluster.sh health
./trigger-http.sh alice hello
./trigger-batch.sh 9
```

| Node | QUIC | Admin | Gateway |
|------|------|-------|---------|
| 1 | `:7743` | `:9380` | `:8294` |
| 2 | `:7753` | `:9381` | `:8295` |
| 3 | `:7763` | `:9382` | `:8296` |

Connect to any node's gateway URL; sessions stick to a worker instance cluster-wide. Forward **8294** (or 8295/8296) and **9380** in Cursor/SSH.

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `CRAFTY_GATEWAY` | `127.0.0.1:8294` | HTTP/WS bind (`-` disables gateway on a node) |
| `CRAFTY_PEERS` | unset | When set → QUIC cluster mode |
| `GATEWAY_TOKEN` | unset | When set, require `?token=` or matching Bearer |
| `CRAFTY_DATA_DIR` | `/tmp/crafty-showcase-realtime` | Cluster + actor data |

Guide: [docs/scenarios/realtime-sessions.md](../../docs/scenarios/realtime-sessions.md)
