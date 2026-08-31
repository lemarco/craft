# Real-time / session — sticky actors + stateless gateway

**Pattern:** WebSocket or long-lived HTTP to a **pinned worker**; gateway VPS stays stateless; workers scale on the cluster.

**Status:** **Shipped** in 0.2.x — `ActorSession`, gateway showcase ([examples/realtime/](../../examples/realtime/)), `ActorsApi` on product gateway.

## When to use

- Chat, collaborative editing, game session, live notifications
- Client must hit the **same actor instance** for a period (in-memory state)
- Gateway can sit behind a load balancer; workers run anywhere in cluster

**Do not** require Redis for session stickiness — use [`ActorSession`](../../crates/crafty-actor/src/session.rs) ([actor-routing-tier3](../decisions/actor-routing-tier3.md)).

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

- **Gateway:** accepts connections, holds `ActorSession`, forwards messages (tier B)
- **Workers:** `UserActor` instances; state in memory for session lifetime
- **Durability:** optional checkpoint to `StateMachine` or `ActorStateStore` (redb) if reconnect must restore history

## Session lifecycle

1. **Open:** keyed pick → `ActorSession` with TTL
2. **Traffic:** all messages via `ask_session` / `cast_session` to pinned `ActorId`
3. **Expire / migrate:** session invalid → return `NoTarget`; client re-opens session (may land on new instance)
4. **Scale:** consistent-hash ring remaps ~`1/N` keys ([actor-routing-tier3](../decisions/actor-routing-tier3.md))

## Quick start (current API)

### 1. Cluster + workers

```rust
CraftyCluster::builder(node_id, machine)
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

From messaging / directory ([`ClusterMessaging`](../../crates/crafty-actor/src/messaging.rs)):

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

Facade helpers may wrap `ClusterRef` — see `cluster_ref_routing` example.

### 3. Gateway role (same binary, env flag)

Recommended deployment:

| Env | Role |
|-----|------|
| `GATEWAY=1` | Bind public HTTP/WebSocket; no local workers required |
| default | Run workers + optional admin |

Same artifact on every VPS; LB round-robins **gateways only**. Workers communicate over existing mTLS peer paths ([wire-protocol](../decisions/wire-protocol.md)).

### 4. WebSocket handler (see showcase)

Full runnable example: [`examples/realtime/`](../../examples/realtime/). Sketch:

```rust
async fn on_ws(socket: WebSocket, cluster: Arc<CraftyCluster<MySm>>) {
    let user_id = authenticate(&socket).await?;
    let session = open_session(&cluster, user_id).await?;

    while let Some(Ok(frame)) = socket.next().await {
        let msg = decode(frame)?;
        let reply = cluster.messaging().ask_session(&session, msg).await?;
        socket.send(encode(reply)).await?;
    }
}
```

Gateway does **not** hold conversation state — only the session handle.

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
| LB | UDP/TCP to gateway `:7443` / admin `:8080`; workers need not be public |
| Rate limit | `TrafficPolicy` on client/actor classes ([future-work-and-risks](../decisions/future-work-and-risks.md) R2) |
| Drain | Per-group `set_group_drain_timeout` for long sessions |

## Examples

| Asset | Purpose |
|-------|---------|
| `crafty-actor/tests/messaging.rs` — `cast_session` | ✅ |
| [`examples/realtime/`](../../examples/realtime/) | WebSocket + `ActorSession` showcase |
| `crafty/tests/http_actors.rs` | HTTP cast/ask on gateway |

## Future polish

Gateway auth beyond `GATEWAY_TOKEN` query param — apps add JWT/API keys in custom routes.

## Related

- [actor-routing-tier3](../decisions/actor-routing-tier3.md)
- [cross-node-actors](../decisions/cross-node-actors.md)
- [stateful-workers](stateful-workers.md) — durable history
- [backlog.md](../backlog.md) — B-04
