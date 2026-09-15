# WebSocket wiring

How to add WebSocket endpoints on the product HTTP listener (`TREMBITA_LISTEN` / `GatewayOpts`).

## Choose an entry point

| You need | API | Where to wire |
|----------|-----|----------------|
| Full control (custom gateway, multi-host) | `mount_raw_websocket` / `mount_sticky_websocket` | `GatewayOpts::surfaces(\|state\| …)` → `RouteTable` on `dev_fallback` or host surface |
| Extra paths on **default** product gateway (ops + jobs + …) | `GatewayOpts::websocket_routes(\|state\| …)` | Builder only — merged with env default surfaces |
| Sticky chat / session actor | `GatewayOpts::realtime_ws` / `realtime_ws_auth` | Builder `.gateway(...)` |
| Ticker feed / notify hub (no custom handler) | `GatewayOpts::ws(WsMount::…)` | Builder `.gateway(...)` |
| WebSocket module | Copy/adapt from [`examples/`](../../examples/) or scaffold `http/` | `src/http/ws.rs` + merge in `app.rs` |

**Rule of thumb:** custom `surfaces` closure → put WS in that `RouteTable`. Env/default gateway → `websocket_routes`, `realtime_ws`, or `ws`.

## Scenarios → API → example

| Scenario | Auth | API | Example |
|----------|------|-----|---------|
| Echo / custom protocol | Open or identity | `mount_raw_websocket` | [`examples/ws-minimal/`](../../examples/ws-minimal/) |
| Sticky in-memory actor | Identity (or session) | `mount_sticky_websocket`, `run_sticky_cast_loop` | [`examples/realtime/`](../../examples/realtime/) |
| Public market ticks | Open | `WsBroadcastHub` + `WsSubscribeCmd` | [`examples/market-ws/`](../../examples/market-ws/) |
| Private push per user | Identity | `WsNotifyHub` | [`examples/ws-notify/`](../../examples/ws-notify/) |
| Declarative one-liner | Varies | `GatewayOpts::ws(WsMount::broadcast(…))` | [`examples/market-ws/`](../../examples/market-ws/) |

## Client JSON (broadcast hub)

```json
{"op":"subscribe","topic":"BTC"}
{"op":"unsubscribe","topic":"BTC"}
```

Rust: [`WsSubscribeCmd`](../../crates/trembita/src/gateway/ws/broadcast.rs).

## Dependencies

Product apps need only `trembita` with `http-jobs`. Use `trembita::tokio_tungstenite`, `trembita::futures_util`, `trembita::WsMessage` — do not add separate `tokio-tungstenite` unless you want a different version.

## Local dev

| Example | Command |
|---------|---------|
| Cluster showcases | `./target/debug/trembita dev up --showcase realtime` (debug CLI; repo root) |
| Solo HTTP WS demos | `./target/debug/trembita dev up --showcase ws-minimal` (or `market-ws`, `ws-notify`) |

Solo showcases bind HTTP on the showcase `base_port` only (no multi-node QUIC layout).

## Low-level (advanced)

Multiple paths on one listener: register several `.websocket*` routes on the same [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) (merge combines them). Auth modes: [`AuthMode::Open`](../../crates/trembita-http/src/routing/auth.rs), `Identity`, `Session` — match your HTTP routes.

See also [realtime-sessions](realtime-sessions.md), [gateway-identity](../decisions/gateway-identity.md).
