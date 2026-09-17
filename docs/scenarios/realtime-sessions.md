# Real-time / session — sticky sessions + stateless gateway

**Pattern:** WebSocket or long-lived HTTP to a **pinned capability group** (`Route::Session`); gateway stays stateless; group hosts scale on the cluster ([product-terminology](../decisions/product-terminology.md)).

**Status:** **Shipped** in 0.2.x — `ActorSession`, gateway showcase ([examples/realtime/](../../examples/realtime/)) on **capabilities** (`chat.append` + `Route::Session`). See [capability-parity](capability-parity.md).

## When to use

- Chat, collaborative editing, game session, live notifications
- Client must hit the **same group host instance** for a period (in-memory session state)
- Gateway can sit behind a load balancer; workers run anywhere in cluster

**Do not** require Redis for session stickiness — use [`ActorSession`](../../crates/trembita-runtime/src/session.rs) ([actor-routing](../decisions/actor-routing.md)).

## Architecture

```
                    ┌─────────────────┐
  Clients ──WS──►   │ Gateway VPS     │  stateless — any node
                    │  (same binary)  │
                    └────────┬────────┘
                             │ ask_session / cast_session
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         chat host-0    chat host-1    chat host-2
         (VPS 1)        (VPS 2)        (VPS 3)
         CapHost RAM    CapHost RAM    CapHost RAM
```

- **Gateway:** accepts connections, holds [`SessionHandle`](../../crates/trembita/src/gateway/session.rs), forwards typed ops to pinned hosts
- **Group host:** internal `CapHost`; in-memory state for session lifetime
- **Durability:** optional checkpoint to `StateMachine` or **cap store** (redb) if reconnect must restore history

## Session lifecycle

1. **Open:** keyed pick → `ActorSession` with TTL
2. **Traffic:** all messages via `ask_session` / `cast_session` to pinned `ActorId`
3. **Expire / migrate:** session invalid → return `NoTarget`; client re-opens session (may land on new instance)
4. **Scale:** consistent-hash ring remaps ~`1/N` keys ([actor-routing](../decisions/actor-routing.md))

## Quick start (current API)

**Product path:** register `chat` in [`CapManifest`](../../crates/trembita/src/capability/manifest.rs), sticky WebSocket with `mount_sticky_websocket`, deliver lines via [`SessionHandle::fire_cap`](../../crates/trembita/src/gateway/session.rs) — see [`examples/realtime/`](../../examples/realtime/) and `trembita new --template realtime`.

### 1. Manifest (capability group)

```rust
// capabilities/chat.rs
capabilities::chat::manifest() // CapGroup + `.op_req(append, [Session, InlineFire])`
```

### 1b. Product boot (same as other scenarios)

```rust
TrembitaApp::from_env()?
    .manifest(
        AppManifest::new()
            .capabilities(/* chat CapGroup */)
            .workers(/* session workers */),
    )
    .configure(TrembitaConfigure::default().with_local_gateway_apis())
    .run()
    .await?;
```

Custom cluster assembly with a non-empty Raft SM is **not** public API — see [public-api-1.0](../decisions/public-api-1.0.md).

### 2. Open sticky session

From messaging / directory ([`ClusterMessaging`](../../crates/trembita-runtime/src/messaging.rs)):

```rust
use std::time::Duration;

let session = cluster
    .messaging()
    .directory()
    .session_keyed(&user_id, Some(Duration::from_secs(3600)))
    .expect("no worker for key");

let reply = cluster
    .messaging()
    .ask_session(&session, ChatMsg { text: "hello" })
    .await?;
```

Facade helpers may wrap `ClusterRef` — see [`examples/realtime/`](../../examples/realtime/).

### 3. Gateway role (same binary, env flag)

Recommended deployment:

| Env | Role |
|-----|------|
| `GATEWAY=1` | Bind public HTTP/WebSocket; no local workers required |
| default | Run workers + optional admin |

Same artifact on every VPS; LB round-robins **gateways only**. Workers communicate over existing mTLS peer paths ([wire-protocol](../decisions/wire-protocol.md)).

### 4. WebSocket handler (gateway identity + session)

Decision: [gateway-identity](../decisions/gateway-identity.md). Full example: [`examples/realtime/`](../../examples/realtime/).

HTTP handlers are counted automatically by gateway middleware ([`build_gateway_service`](../../crates/trembita/src/gateway/mod.rs)). WebSocket sessions must call [`track_connection`](../../crates/trembita/src/gateway/mod.rs) inside the upgrade callback — the HTTP upgrade response returns before the socket closes.

```rust
use std::time::Duration;
use trembita::{
    Gateway, GatewayIdentity, GatewayOpts, GatewayRequest, SessionHandle, SessionKey,
    TrembitaGatewayState,
};
use trembita_http::{RouteTable, accept_websocket, routing_to_http_response};

// Your auth (JWT, cookie→DB, …) — trembita only calls extract().
struct AppIdentity { /* db, jwt, … */ }
impl GatewayIdentity for AppIdentity {
    type Identity = UserId;
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<UserId, trembita::IdentityError> {
        /* … */
    }
}
impl SessionKey for UserId {
    fn session_key(&self) -> std::borrow::Cow<'_, str> {
        self.0.to_string().into()
    }
}

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(RouteTable::new().websocket("/ws", move |req| {
        let st = state.clone();
        Box::pin(async move {
            match st
                .open_worker_session_parts(
                    "chat",
                    req.method(),
                    req.uri(),
                    req.headers(),
                    Some(Duration::from_secs(3600)),
                )
                .await
            {
                Ok(handle) => accept_websocket(req, move |stream| {
                    let st = st.clone();
                    async move {
                        let _guard = st.track_connection();
                        // tokio_tungstenite::WebSocketStream::from_raw_socket(stream, …)
                        let _ = (stream, handle);
                    }
                }),
                Err(e) => routing_to_http_response(e.into_http_response()),
            }
        })
    }))
}

TrembitaApp::builder()
    .gateway(
        GatewayOpts::from_env()? // or GatewayOpts::new(addr) when TREMBITA_LISTEN == addr
            .identity(AppIdentity { /* … */ })
            .surfaces(gateway_surfaces),
    );
```

Gateway does **not** hold conversation state — only the session handle ([`SessionHandle`](../../crates/trembita/src/gateway/session.rs)).

### 5. HTTP handlers (same identity)

Prefer **cookie session + typed op** (no raw mailbox bytes). Reference: [`examples/realtime/src/gateway_session.rs`](../../examples/realtime/src/gateway_session.rs) — `SessionHandle::open_for::<Append>` then [`fire_cap`](../../crates/trembita/src/gateway/session.rs).

Showcases: [`examples/realtime`](../../examples/realtime/) (`POST /chat`, `GET /me` via `fire_cap`), [`examples/stateful-workers`](../../examples/stateful-workers/) (`POST /orders/submit` via `cap_fire` — no `/actors/*` on default gateway).

```rust
let mut handle = SessionHandle::open_for::<Append>(&state.app, session_key, Some(ttl))
    .ok_or_else(|| HttpError::Internal("no chat host".into()))?;
handle.fire_cap(Append { text: body.message }).await?;
```

Legacy [`open_actor_session_parts`](../../crates/trembita/src/gateway/state.rs) (deprecated) alias [`open_worker_session_parts`](../../crates/trembita/src/gateway/state.rs); greenfield uses capability session mounts + `fire_cap` ([capability-greenfield-wire](../decisions/capability-greenfield-wire.md), [gateway-session-naming](../decisions/gateway-session-naming.md)).

Integration tests: [`trembita/tests/gateway_http.rs`](../../crates/trembita/tests/gateway_http.rs).

## Consistency choices

| Need | API |
|------|-----|
| Fast reply, local actor state | `ask_session` (default) |
| Fresh directory after spawn | `DirectoryPolicy::ReadYourWrites` |
| Linearizable domain read | Raft `query` on SM — not actor memory |

See [read-consistency](../decisions/client-and-routing.md#read-consistency).

## Reconnect strategy

| Strategy | Trade-off |
|----------|-----------|
| Client resends `session_id` cookie; server re-keyed pick | May land on different worker — load history from SM/redb |
| Long TTL on `ActorSession` | Stale pin if worker died — handle `NoTarget` |
| Persist transcript in SM | Heavier writes; full durability |

## Optional durability

| Data | Store |
|------|-------|
| Live typing indicators | Worker memory only |
| Message history | `StateMachine` or redb store ([stateful-workers](stateful-workers.md)) |
| Presence | Actor or SM |

## Operations

| Concern | Action |
|---------|--------|
| LB | One port (e.g. `:443`) — QUIC (UDP) + HTTP/WS (TCP) on the same number; workers need not be public |
| Rate limit | `TrafficPolicy` on client/actor classes ([future-work-and-risks](../decisions/future-work-and-risks.md) R2) |
| Drain | Per-group `set_group_drain_timeout` for long sessions |

## Examples

| Asset | Purpose |
|-------|---------|
| `trembita-runtime/tests/messaging.rs` — `cast_session` | ✅ |
| [`examples/realtime/`](../../examples/realtime/) | WebSocket + `ActorSession` showcase |
| `trembita/tests/http_actors.rs` | HTTP cast/ask on gateway |

## Future polish

Gateway auth: [`GatewayBearerIdentity`](../../crates/trembita/src/gateway/identity.rs) on [`GatewayOpts::identity`](../../crates/trembita/src/gateway/opts.rs) plus [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs) on sticky WebSocket and login routes (see [`examples/realtime/`](../../examples/realtime/)). Custom routes use the same identity via [`TrembitaGatewayState::open_worker_session_parts`](../../crates/trembita/src/gateway/state.rs).

## Related

- [actor-routing](../decisions/actor-routing.md)
- [cross-node-actors](../decisions/cross-node-actors.md)
- [stateful-workers](stateful-workers.md) — durable history
- [status.md](../status.md) — sessions / gateway · [examples/realtime/](../../examples/realtime/)
