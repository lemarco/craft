# Observability & monitoring (BEAM-style)

**Status:** Accepted  
**Date:** 2026-07-05

## Context

craft is actor-native on `ractor` (modeled on Erlang/OTP). The user wants **BEAM-level monitoring** — everything Observer / LiveDashboard / `:telemetry` / supervision offers, mapped to craft's distributed cross-node model.

User chose **ALL** proposed capabilities in v1 (with tracing/dashboard performance caveats).

## Decision

Ship a **full observability stack**:

1. Structured `tracing` everywhere
2. Prometheus `/metrics`
3. Actor **telemetry event stream**
4. Cluster **introspection JSON API**
5. Exposed **supervision / restart policies**
6. **Live web dashboard** (read-only)
7. Opt-in **message tracing**

All read surfaces live on the **admin port** ([wire-protocol](wire-protocol.md#admin-http-port--8080tcp), default `:8080`), never on the mTLS craft wire.

---

### 1. Tracing

`tracing` spans across `craft-core`, `craft-net`, `craft-actor`. Correlated by `NodeId`, `ActorId`, `req_id`. Configurable level via `RUST_LOG` / `CRAFT_LOG`.

### 2. Metrics (`GET /metrics`, Prometheus)

| Domain | Metrics |
|--------|---------|
| Raft | term, role, commit_index, election_count, append_latency, leader_changes |
| Cluster | live_nodes, membership_changes, join/leave counts |
| Actors | actor_count, mailbox_depth, message_rate, handle_latency, restarts, migrations |
| Client | request_rate, forward_count, readindex_latency |
| Store | redis_ops, redis_errors ([actor-state-redis](actor-state-redis.md)) |

Always-on and cheap (counters/gauges/histograms).

### 3. Telemetry event stream

BEAM `:telemetry`-style events emitted from the runtime:

```rust
pub enum CraftEvent {
    ActorSpawned { id: ActorId },
    ActorStopped { id: ActorId, reason: StopReason },
    ActorRestarted { id: ActorId, count: u32 },
    ActorMigrated { id: ActorId, from: NodeId, to: NodeId },
    MailboxDepth { id: ActorId, len: usize },
    MessageHandled { id: ActorId, latency: Duration },
    NodeJoined { node_id: NodeId },
    NodeLeft { node_id: NodeId, graceful: bool },
    LeaderChanged { term: Term, leader: NodeId },
}
```

User subscribes:

```rust
let mut events = cluster.events().subscribe();
while let Some(ev) = events.recv().await { /* forward to sink */ }
```

Backed by a broadcast channel; drops for slow consumers are counted (never block actors).

### 4. Introspection API (Observer-like)

Read-only cluster/actor state over admin HTTP (JSON):

| Route | Returns |
|-------|---------|
| `GET /introspect/cluster` | nodes, roles, leader, membership |
| `GET /introspect/actors` | all actors: id, node, type, mailbox depth, uptime, generation |
| `GET /introspect/actors/{id}` | single actor detail |
| `GET /introspect/node/{id}` | per-VPS: workers, resources, store health |

Cross-node aggregation: admin queries fan out via existing actor directory ([cross-node-actors](cross-node-actors.md)) / peer RPC; leader can serve cluster-wide view.

### 5. Supervision / restart policies

Expose ractor OTP-style supervision to users:

```rust
registry.spawn::<Worker>("workers", cfg)
    .restart(RestartPolicy::OnFailure { max_restarts: 5, window: Duration::from_secs(60) })?;

pub enum RestartPolicy {
    Never,
    OnFailure { max_restarts: u32, window: Duration },
    Always,
}
```

Restart events surface in telemetry + metrics. Exhausted restart budget → escalate (stop + `CraftEvent::ActorStopped { reason: RestartLimit }`).

### 6. Live web dashboard (`GET /dashboard`)

Read-only UI on admin port (v1, minimal but real):

- Cluster map: nodes, leader, health
- Per-node workers + mailbox depth + message rate
- Live event feed (from telemetry stream via SSE/WebSocket)
- Raft state: term, commit index, recent leader changes

Implementation: small embedded static assets + admin HTTP + SSE. **Read-only** (no cluster mutation from dashboard in v1).

### 7. Message tracing (opt-in)

`dbg`/`recon`-style per-message trace — **off by default** (perf cost):

```rust
cluster.trace().actor(id).enable(TraceOpts { messages: true, duration: 30s })?;
```

Emits trace events to telemetry stream / logs; auto-expires. Never on for whole cluster by default.

---

## Performance caveats (vs BEAM)

- BEAM introspection is VM-native and cheap; Rust equivalents cost more.
- **Always-on:** metrics + high-level telemetry (counters, mailbox gauges).
- **Opt-in:** per-message tracing, live state dumps.
- Telemetry uses bounded broadcast; slow subscribers drop (counted), never block the actor mailbox or Raft loop.

## Crate impact

| Crate | Add |
|-------|-----|
| `craft-actor` | telemetry emitter, mailbox metrics, restart policy |
| `craft-net` | admin routes: `/metrics`, `/introspect/*`, `/dashboard`, SSE |
| `craft-core` | Raft metrics + events |
| `craft` (facade) | `cluster.events()`, `cluster.introspect()`, `cluster.trace()` |
| `craft-dashboard` (optional) | embedded UI assets |

## Consequences

**Positive**

- BEAM-class visibility: metrics, events, introspection, dashboard, supervision
- Works with standard tooling (Prometheus, Grafana) + built-in UI

**Negative**

- Significant surface area; dashboard + introspection add v1 work
- Must guard performance (opt-in heavy tracing)
- Admin surface must stay private / access-controlled

## Related

- [wire-protocol.md#admin-http-port--8080tcp](wire-protocol.md#admin-http-port--8080tcp)
- [cross-node-actors.md](cross-node-actors.md)
- [cluster-elasticity.md#supervisor--leader-only-reconciliation](cluster-elasticity.md#supervisor--leader-only-reconciliation)
- [actor-state-redis.md](actor-state-redis.md)
