# Observability & monitoring (BEAM-style)

**Status:** Accepted  
**Date:** 2026-07-05

## Context

trembita is actor-native on `ractor` (modeled on Erlang/OTP). Observability targets **BEAM-level monitoring** — metrics, telemetry, introspection, supervision visibility, and an optional live dashboard — mapped to trembita's distributed cross-node model, with heavy tracing opt-in ([Performance caveats](#performance-caveats-vs-beam)).

## Decision

Ship a **full observability stack**:

1. Structured `tracing` everywhere
2. Prometheus `/metrics`
3. Actor **telemetry event stream**
4. Cluster **introspection JSON API**
5. Exposed **supervision / restart policies**
6. **Live web dashboard** (read-only)
7. Opt-in **message tracing**

All read surfaces live on the **ops HTTP bind** ([wire-protocol](wire-protocol.md#ops-http-tcp-on-trembita_listen) — for product apps, TCP on **`TREMBITA_LISTEN`**), never on the mTLS trembita wire.

---

### 1. Tracing

`tracing` spans across `trembita-core`, `trembita-net`, `trembita-runtime`. Correlated by `NodeId`, `ActorId`, `req_id`. Configurable level via `RUST_LOG` / `TREMBITA_LOG`.

### 2. Metrics (`GET /metrics`, Prometheus)

| Domain | Metrics |
|--------|---------|
| Raft | term, role, commit_index, election_count, append_latency, leader_changes |
| Cluster | live_nodes, membership_changes, join/leave counts |
| Actors | actor_count, mailbox_depth, message_rate, handle_latency, restarts, migrations |
| Client | request_rate, forward_count, readindex_latency |
| Store | redis_ops, redis_errors ([actor-state-redis](actor-state-redis.md)) |

Always-on and cheap (counters/gauges/histograms).

#### Metrics export port

Pull via `GET /metrics` remains the default path (Prometheus scrape). For push
backends, implement [`MetricsSink`](../../crates/trembita-dashboard/src/metrics_sink.rs)
and wire it on the cluster/app builder:

```rust
cluster.metrics_sink(Arc::new(my_sink));
// or: TrembitaApp::builder().metrics_sink(Arc::new(my_sink))
```

The in-process Prometheus registry is **always** updated in parallel; the sink
receives the same `incr` / `set` / `observe` samples. OpenTelemetry / OTLP push
export lives in optional [`trembita-metrics-otlp`](../../crates/trembita-metrics-otlp/)
(facade feature `otlp-metrics` → [`init_metrics_with_otlp`](../../crates/trembita-metrics-otlp/src/lib.rs));
not in the default dependency tree.

### 3. Telemetry event stream

BEAM `:telemetry`-style events emitted from the runtime:

```rust
pub enum TrembitaEvent {
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

Read-only cluster/actor state over ops HTTP (JSON):

| Route | Returns |
|-------|---------|
| `GET /introspect/cluster` | nodes, roles, leader, membership |
| `GET /introspect/actors` | all actors: id, node, type, mailbox depth, uptime, generation |
| `GET /introspect/actors/{id}` | single actor detail |
| `GET /introspect/node/{id}` | per-VPS: workers, resources, store health |

Cross-node aggregation: ops handlers fan out via existing actor directory ([cross-node-actors](cross-node-actors.md)) / peer RPC; leader can serve cluster-wide view.

**Product gateway:** teams with a custom operator UI can mount the same JSON beside product routes via [`IntrospectApi`](introspect-api.md) (`RouteTable` merge or default gateway surfaces). [`TrembitaApp::from_env()`](../../crates/trembita/src/app/runtime.rs) already serves `/introspect/*` on the unified bind; use [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs) when exposing snapshots on a public hostname.

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

Restart events surface in telemetry + metrics. Exhausted restart budget → escalate (stop + `TrembitaEvent::ActorStopped { reason: RestartLimit }`).

### 6. Live web dashboard (`GET /dashboard`)

Read-only UI on the ops HTTP bind (minimal but real):

- Cluster map: nodes, leader, health
- Per-node workers + mailbox depth + message rate
- Live event feed (from telemetry stream via SSE/WebSocket)
- Raft state: term, commit index, recent leader changes

Implementation: small embedded static assets + ops HTTP + SSE. **Read-only** — no cluster mutation from the dashboard.

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
| `trembita-runtime` | telemetry emitter, mailbox metrics, restart policy |
| `trembita-http` / gateway | ops routes on unified bind: `/metrics`, `/introspect/*`, `/dashboard`, SSE |
| `trembita-core` | Raft metrics + events |
| `trembita` (facade) | `cluster.events()`, `cluster.introspect()`, `cluster.trace()` |
| `trembita-dashboard` (optional) | embedded UI assets |

## Consequences

**Positive**

- BEAM-class visibility: metrics, events, introspection, dashboard, supervision
- Works with standard tooling (Prometheus, Grafana) + built-in UI

**Negative**

- Large surface area to maintain (metrics, introspection, dashboard)
- Must guard performance (opt-in heavy tracing)
- Ops HTTP must stay private / access-controlled on production networks

## Related

- [wire-protocol.md#ops-http-tcp-on-trembita_listen](wire-protocol.md#ops-http-tcp-on-trembita_listen)
- [cross-node-actors.md](cross-node-actors.md)
- [cluster-elasticity.md#supervisor--leader-only-reconciliation](cluster-elasticity.md#supervisor--leader-only-reconciliation)
- [actor-state-redis.md](actor-state-redis.md)
