# Real-time sessions (capability + sticky session)

WebSocket **and authenticated HTTP** on one listener → sticky session → **`Append`** via [`SessionHandle::fire_cap`](../../crates/trembita/src/gateway/session.rs) (`chat.append` capability).

Ops routes (`/health`, `/dashboard`, …) are merged on the same bind — same model as [getting-started.md](../../docs/getting-started.md).

See [capability-parity](../../docs/scenarios/capability-parity.md) for session + WebSocket contract.

## What you run

| Piece | Role |
|-------|------|
| This binary | WS + HTTP product routes + ops on `GatewayOpts` |
| [`trigger.sh`](trigger.sh) | One WS message via `trembita-showcase-client` or [websocat](https://github.com/vi/websocat) |
| [`trigger-http.sh`](trigger-http.sh) | Session flow: login cookie → `POST /chat` |
| [`trigger-batch.sh`](trigger-batch.sh) | Multi-user chat burst |

## Quick start (local — one terminal)

**Terminal 1:**

```bash
cd examples/realtime
cargo run --release
```

**Terminal 2:**

```bash
./trigger.sh alice hello                    # WebSocket (?user= or Bearer)
./trigger-http.sh alice hello               # login + session cookie + POST /chat
curl 'http://127.0.0.1:8290/me?user=alice'  # GET /me (identity)
curl -s http://127.0.0.1:8290/health        # ops on same port
./trigger-batch.sh 6
```

Manual WebSocket:

```bash
websocat 'ws://127.0.0.1:8290/ws?user=alice'
```

Manual HTTP with Bearer (recommended when `GATEWAY_TOKEN` is set):

```bash
curl -X POST 'http://127.0.0.1:8290/login' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -H 'X-Trembita-User: alice'

curl -X POST 'http://127.0.0.1:8290/chat' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -H 'X-Trembita-User: alice' \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello"}'
```

Dev without a token: `?user=alice` on WebSocket and HTTP still works.

## Gateway routes

| Route | Auth | Body |
|-------|------|------|
| `GET /ws` | `?user=` or Bearer + `X-Trembita-User` | WebSocket upgrade |
| `POST /login` | Bearer + `X-Trembita-User` | issues `Set-Cookie: sess=…` |
| `POST /chat` | session cookie (after login) or Bearer/`?user=` | `{"message":"…"}` |
| `GET /me` | session or identity | returns `{"user":"…"}` |
| `GET /health`, `/dashboard`, … | ops (typically open on dev bind) | merged `OpsApi` |

Identity: [`GatewayBearerIdentity::from_env()`](../../crates/trembita/src/gateway/identity.rs) on
[`GatewayOpts::identity`](../../crates/trembita/src/gateway/mod.rs). Product routes use
`post_identity`, `post_session`, `get_session`, and `dev_fallback_session` (see `src/main.rs`).

Optional session cookie tuning: `REALTIME_SESSION_COOKIE`, `REALTIME_SESSION_TTL` ([`CookieConfig`](../../crates/trembita-http/src/cookie_config.rs)).

## Quick start (cluster — QUIC)

Three **identical** nodes — each runs WS + HTTP + chat capability:

```bash
cd examples/realtime
./cluster.sh setup
./cluster.sh up
./cluster.sh health
./trigger-http.sh alice hello
./trigger-batch.sh 9
```

| Node | `TREMBITA_LISTEN` (QUIC + HTTP/WS) |
|------|-------------------------------------|
| 1 | `:8290` |
| 2 | `:8291` |
| 3 | `:8292` |

Connect to any node's URL; sessions stick to a worker instance cluster-wide. Forward **8290** (or 8291/8292) in Cursor/SSH.

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `TREMBITA_LISTEN` | `127.0.0.1:8290` | One port — QUIC wire + HTTP/WS product + ops |
| `TREMBITA_JOIN_SEEDS` | unset | Joiners: `1@127.0.0.1:8290` (see `./cluster.sh`) |
| `TREMBITA_HTTP` | *(omit)* | `-` disables TCP only (QUIC-only node) |
| `GATEWAY_TOKEN` | unset | When set, require matching Bearer (or legacy `?token=` on WS) |
| `TREMBITA_DATA_DIR` | `/tmp/trembita-showcase-realtime` | Persisted node id + actor data |

Guide: [docs/scenarios/realtime-sessions.md](../../docs/scenarios/realtime-sessions.md)
