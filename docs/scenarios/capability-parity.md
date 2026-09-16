# Capability parity — same power, better DX

**Principle:** Product scenarios must not regress when using [`CapManifest`](../decisions/capability-dx.md). App code avoids [`UserActor`](../../crates/trembita-runtime/src/user_actor.rs); the runtime still uses internal [`CapHost`](../../crates/trembita/src/capability/host.rs) (mailbox, sessions, placement).

See [capabilities.md](capabilities.md) for API sketch and [capability-greenfield-wire](../decisions/capability-greenfield-wire.md) for HTTP policy.

## Scenario matrix

| Scenario | Capability recipe | Routes | HTTP (gateway) | `UserActor` in app |
|----------|-------------------|--------|----------------|---------------------|
| Short RPC / CRUD | `CapOp` + keyed `.key()` | `Inline` | `cap_invoke` | No |
| Fire-and-forget command | same op | `InlineFire` | `cap_fire` | No |
| Background / retry work | same op + `.queue_stream()` | `Queued`, `QueuedWait` | `cap_enqueue` / client `.queued_wait()` | No |
| Delayed work | same op + queue | `Scheduled` | cron + enqueue or client `.schedule()` | No |
| Domain fan-out | `.event_ingress(topic, sub)` | `Event` | publish API + `.publish_event()` | No |
| Idempotent side effects | handler + `OpCtx` / `ActorStateStore` | any | same as route | No |
| Sticky live traffic (HTTP) | same group name as session | `Session` | session cookie + `SessionHandle` or `.session_key(k).route(Route::Session)` | No |
| Sticky WebSocket | group = WS mount name; payload = [`CapWire`](../../crates/trembita/src/capability/wire.rs) | `Session` (+ `InlineFire` for cast path) | `mount_sticky_websocket` | No |
| Raft domain entities | `cluster.propose` / SM | — | custom or ops | No |
| RAM snapshot migration | — | — | — | **Yes** (advanced lab) |
| Custom mailbox protocol | — | — | — | **Yes** (escape hatch) |
| Saga step → side effect | call `Req::via(app).route(Inline)` from step handler | `Inline` | optional | No (preferred) |
| Job consumer → side effect | `Req.via(app).fire()` from `#[consumer]` via `on_app` bridge | `InlineFire` | — | No |

## Session + WebSocket contract

Sticky [`SessionHandle::cast`](../../crates/trembita/src/gateway/session.rs) delivers **postcard [`CapWire`](../../crates/trembita/src/capability/wire.rs)** to the group host when the group is a capability (`CapHost`). Raw `String` / ad-hoc bytes worked on custom `UserActor` decoders only.

Sticky session (showcase [`examples/realtime`](../../examples/realtime/)) — **typed only**, wire framing is internal:

```rust
handle.fire_cap(Append { text }).await?;
// or
Append { text }.via(&app).session_key(user).route(Route::Session).await?;
```

## Workflows

Saga steps invoke **capability ops** ([`Route::Inline`](../../crates/trembita/src/capability/route.rs)) from the workflow client — see [`examples/workflows`](../../examples/workflows/) (`OnboardingWorkflowClient` → `CreateAccount { .. }.via(&app).route(Route::Inline)`). Step payloads stay postcard-encoded for the journal; handlers live in `capabilities/onboarding.rs`.

## When `UserActor` stays public

| Need | Why capability is not enough |
|------|------------------------------|
| `#[actor(migratable)]` RAM snapshot | Not store/idempotency semantics |
| Custom `decode_message` / non-`CapWire` frames | Outside capability envelope |
| Realtime template chat actor (legacy) | Removed — `trembita new --template realtime` uses cap `chat` |

Internal: every capability group **is** a `UserActor` host — users never implement it.

## Related

- [stateful-workers](stateful-workers.md) — store idempotency (default migration story)
- [realtime-sessions](realtime-sessions.md) — sessions + WS
- [background-jobs](background-jobs.md) — queue semantics for `Route::Queued`
- [backlog B-24](../backlog.md#b-24--capability-parity-no-regression)
