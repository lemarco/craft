# Wire protocol — transport, serialization & ports

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-09-15 — ops HTTP co-hosted on `TREMBITA_LISTEN` ([unified-listener](unified-listener.md))

## Context

All network I/O uses one stack: **HTTP/3 over QUIC** with **postcard** bodies. Operators need predictable default ports and a **plain HTTP/1.1** surface for health, metrics, and product APIs without speaking trembita's mTLS wire.

## Transport — HTTP/3 everywhere

**All network I/O uses HTTP/3 over QUIC.** No separate TCP stack for Raft internals.

| Traffic | Transport | Codec |
|---------|-----------|-------|
| Raft peer RPC | HTTP/3 | `postcard` body |
| Remote client API | HTTP/3 | `postcard` body |
| In-process client (`ClientHandle`) | `ractor` | native types |

**One QUIC listener per node.** Path-based routing distinguishes peer vs client traffic.

### Routes (v1)

```
POST /raft/v1/peer/wire     # inter-node; mTLS with peer identity
POST /raft/v1/client/wire   # client API; mTLS in production
```

Each HTTP request is **one RPC round-trip**. Long-lived **QUIC connections** between peers amortize handshake cost.

| HTTP (client path) | Meaning |
|--------------------|---------|
| `200` + `application/x-postcard` | Handled or proxied |
| `503` / `504` | No leader or forward timeout |

### Stack

| Crate | Role |
|-------|------|
| `quinn` | QUIC transport |
| `h3`, `h3-quinn` | HTTP/3 |
| `rustls` | TLS 1.3 |
| `postcard` | Request/response bodies |

`trembita-net` owns HTTP/3 server, peer pool, outbound client. Peer pool: one QUIC connection per remote `NodeId`; peer RPC uses a dedicated connection separate from client/actor traffic (R2 mitigation).

**Rejected:** gRPC, framed TCP + postcard, HTTP/3 client only with TCP for peers.

## Serialization — postcard

Use **`postcard`** with **`serde`** for all hot-path wire bodies.

| Use | Encoding |
|-----|----------|
| `/raft/v1/peer/wire` | `postcard(PeerWireMessage)` |
| `/raft/v1/client/wire` | `postcard(ClientRequest)` / `postcard(ClientResponse)` |
| SM command/query payloads | User types via serde inside `ClientRequest::payload` |

**HTTP header:** `Content-Type: application/x-postcard`

Centralize encode/decode in `trembita-proto/src/codec.rs`. Not self-describing — wire compatibility requires matching Rust types.

Optional dev-only JSON for debugging may be added later; not the default wire format.

## Default listen port — 7443/udp

**Default listen address:** `0.0.0.0:7443` (UDP, HTTP/3).

| Source | Key | Default |
|--------|-----|---------|
| Builder | `.listen(addr)` | `0.0.0.0:7443` if omitted |
| Environment | `LISTEN_ADDR` / `TREMBITA_LISTEN` | same |

All trembita wire traffic on one listener: peer, client, join, actor routes. Firewall: open **UDP 7443** (or chosen port) for peer and client mTLS.

## Ops HTTP (TCP on `TREMBITA_LISTEN`)

**Product apps:** one published **`host:port`** — QUIC wire (UDP) and ops + product HTTP (TCP) share the **port number** ([env.md](../env.md)). [`TrembitaApp::from_env()`](../../crates/trembita/src/app/runtime.rs) mounts `/health`, `/ready`, `/metrics`, `/dashboard`, and `/introspect/*` on that TCP bind unless you call [`.without_ops()`](../../crates/trembita/src/app/builder.rs).

| Route | Meaning | 200 when |
|-------|---------|----------|
| `GET /health` | Liveness | Process alive |
| `GET /ready` | Readiness | Raft member, not draining, auto workers spawned |
| `GET /metrics` | Prometheus | Always |
| `GET /introspect/*` | Cluster/actor introspection | Always |
| `GET /dashboard` | Live monitoring UI | Always |

Responses: plain HTTP, JSON (ops HTTP is **not** the postcard hot path).

| Source | Key | Default |
|--------|-----|---------|
| Product env | `TREMBITA_LISTEN` | `0.0.0.0:443` (wire + TCP) |
| Internal | `TREMBITA_HTTP` / `TREMBITA_GATEWAY` | omit in deploys; `-` disables TCP only (QUIC-only node) |
| Reference `trembita-node` | same rules via [`product_http_from_wire`](../../crates/trembita/src/env_config.rs) | TCP co-hosted on `TREMBITA_LISTEN` when not disabled |

No consensus / client data on this listener. Optional server-only TLS via `TREMBITA_HTTP_TLS_*` (aliases `TREMBITA_GATEWAY_TLS_*`). No mTLS requirement. Brownfield merges: [unified-listener](unified-listener.md), [migration/unified-listener-0.5.md](../migration/unified-listener-0.5.md).

## Product gateway

Same TCP listener as ops when co-hosted; host-based surfaces via [`GatewayOpts::surfaces`](../../crates/trembita/src/gateway/opts.rs). Default plain HTTP / WS. WebSocket upgrades use **WSS** on a TLS listener. No client certificates.

## Consequences

**Positive:** Single transport story; compact binary codec; standard LB probes on the unified TCP bind; debuggable HTTP semantics.

**Negative:** UDP/QUIC may be blocked on some networks; heavier than raw TCP; publishing one port still means UDP + TCP firewall rules; `h3` ecosystem still maturing.

## Related

- [client-and-routing.md](client-and-routing.md)
- [security.md](security.md)
- [certificates.md](certificates.md)
- [protocol.md](../protocol.md)
