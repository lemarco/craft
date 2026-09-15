# Real-time / session — sticky actors + stateless gateway

**Pattern:** WebSocket or long-lived HTTP to a **pinned worker**; gateway VPS stays stateless; workers scale on the cluster.

**Status:** **Shipped** in 0.2.x — `ActorSession`, gateway showcase ([examples/realtime/](../../examples/realtime/)), `ActorsApi` on product gateway.

## When to use

- Chat, collaborative editing, game session, live notifications
- Client must hit the **same actor instance** for a period (in-memory state)
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
         ChatWorker-0   ChatWorker-1   ChatWorker-2
         (VPS 1)        (VPS 2)        (VPS 3)
         in-memory      in-memory      in-memory
         session state  session state  session state
```

- **Gateway:** accepts connections, holds `ActorSession`, forwards messages to pinned workers
- **Workers:** `UserActor` instances; state in memory for session lifetime
- **Durability:** optional checkpoint to `StateMachine` or `ActorStateStore` (redb) if reconnect must restore history

## Session lifecycle

1. **Open:** keyed pick → `ActorSession` with TTL
2. **Traffic:** all messages via `ask_session` / `cast_session` to pinned `ActorId`
3. **Expire / migrate:** session invalid → return `NoTarget`; client re-opens session (may land on new instance)
4. **Scale:** consistent-hash ring remaps ~`1/N` keys ([actor-routing](../decisions/actor-routing.md))

## Quick start (current API)

### 1. Cluster + workers

```rust
TrembitaCluster::builder(node_id, machine)
    .auto_workers([AutoWorkerSpec::new("chat", WorkerConfig::default())])
    .directory_policy(DirectoryPolicy::ReadYourWrites)  // optional: fresher directory
    .start_quic(...)
    .await?;
```

Scale chat workers across VPS:

```rust
cluster.scale_cluster::<ChatWorker>("chat", node_count, config).await?;
```

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
                .open_actor_session_parts(
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

Use [`open_actor_session_parts`](../../crates/trembita/src/gateway/mod.rs) when the handler also extracts a JSON body; use [`open_actor_session_from`](../../crates/trembita/src/gateway/mod.rs) / [`extract_session_from`](../../crates/trembita/src/gateway/mod.rs) on plain GET handlers.

Showcases: [`examples/realtime`](../../examples/realtime/) (`POST /chat`, `GET /me`), [`examples/stateful-workers`](../../examples/stateful-workers/) (`POST /orders/submit` beside built-in `/actors/*`).

```rust
use http::StatusCode;
use trembita_http::{RequestCtx, Response};

#[derive(Deserialize)]
struct ChatPost { message: String }

async fn post_chat(state: TrembitaGatewayState, ctx: RequestCtx) -> Result<Response, trembita_http::HttpError> {
    let body: ChatPost = ctx.json()?;
    let mut handle = match state
        .open_actor_session_parts(
            "chat",
            ctx.method(),
            ctx.uri(),
            ctx.headers(),
            Some(Duration::from_secs(3600)),
        )
        .await
    {
        Ok(h) => h,
        Err(e) => return Ok(e.into_http_response()),
    };
    let payload = trembita::proto::encode(&body.message).unwrap();
    handle
        .cast(payload)
        .await
        .map_err(|e| trembita_http::HttpError::Internal(e.to_string()))?;
    Ok(Response::json(StatusCode::OK, serde_json::json!({ "ok": true })))
}

async fn get_me(state: TrembitaGatewayState, ctx: RequestCtx) -> Result<Response, trembita_http::HttpError> {
    match state
        .extract_session_parts(ctx.method(), ctx.uri(), ctx.headers())
        .await
    {
        Ok(id) => Ok(Response::json(
            StatusCode::OK,
            serde_json::json!({ "user": id.session_key() }),
        )),
        Err(e) => Ok(e.into_http_response()),
    }
}
```

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

Gateway auth: [`GatewayBearerIdentity`](../../crates/trembita/src/gateway/identity.rs) on [`GatewayOpts::identity`](../../crates/trembita/src/gateway/opts.rs) plus [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs) on sticky WebSocket and login routes (see [`examples/realtime/`](../../examples/realtime/)). Custom routes use the same identity via [`TrembitaGatewayState::open_actor_session_parts`](../../crates/trembita/src/gateway/mod.rs).

## Related

- [actor-routing](../decisions/actor-routing.md)
- [cross-node-actors](../decisions/cross-node-actors.md)
- [stateful-workers](stateful-workers.md) — durable history
- [backlog.md](../backlog.md) — B-04
