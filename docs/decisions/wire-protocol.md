# Wire protocol — transport, serialization & ports

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-08-28 — merged wire-transport, serialization, default-port, health-admin-port

## Context

All network I/O uses one stack: **HTTP/3 over QUIC** with **postcard** bodies. Operators need predictable default ports and a separate admin surface for health checks without speaking crafty's mTLS wire.

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

`crafty-net` owns HTTP/3 server, peer pool, outbound client. Peer pool: one QUIC connection per remote `NodeId`; peer RPC uses a dedicated connection separate from client/actor traffic (R2 mitigation).

**Rejected:** gRPC, framed TCP + postcard, HTTP/3 client only with TCP for peers.

## Serialization — postcard

Use **`postcard`** with **`serde`** for all hot-path wire bodies.

| Use | Encoding |
|-----|----------|
| `/raft/v1/peer/wire` | `postcard(PeerWireMessage)` |
| `/raft/v1/client/wire` | `postcard(ClientRequest)` / `postcard(ClientResponse)` |
| SM command/query payloads | User types via serde inside `ClientRequest::payload` |

**HTTP header:** `Content-Type: application/x-postcard`

Centralize encode/decode in `crafty-proto/src/codec.rs`. Not self-describing — wire compatibility requires matching Rust types.

Optional dev-only JSON for debugging may be added later; not the default wire format.

## Default listen port — 7443/udp

**Default listen address:** `0.0.0.0:7443` (UDP, HTTP/3).

| Source | Key | Default |
|--------|-----|---------|
| Builder | `.listen(addr)` | `0.0.0.0:7443` if omitted |
| Environment | `LISTEN_ADDR` / `CRAFTY_LISTEN` | same |

All crafty wire traffic on one listener: peer, client, join, actor routes. Firewall: open **UDP 7443** (or chosen port) for peer and client mTLS.

## Admin HTTP port — 8080/tcp

**Separate admin HTTP/1.1 listener**, default **`0.0.0.0:8080/tcp`**, distinct from QUIC port.

| Route | Meaning | 200 when |
|-------|---------|----------|
| `GET /health` | Liveness | Process alive |
| `GET /ready` | Readiness | Raft member, not draining, auto workers spawned |
| `GET /metrics` | Prometheus | Always |
| `GET /introspect/*` | Cluster/actor introspection | Always |
| `GET /dashboard` | Live monitoring UI | Always |

Responses: plain HTTP, JSON (admin is **not** the postcard hot path).

| Source | Key | Default |
|--------|-----|---------|
| Builder | `.admin_listen(addr)` | `0.0.0.0:8080` |
| Environment | `CRAFTY_ADMIN_ADDR` | same |
| Disable | `.admin_disabled()` / `CRAFTY_ADMIN_ADDR=off` | admin off |

Admin carries **no consensus / client data**. Default plain HTTP; optional server-only TLS via `.admin_tls()` / `CRAFTY_ADMIN_TLS_*`. No mTLS requirement. No mutation endpoints in v1.

## Consequences

**Positive:** Single transport story; compact binary codec; standard LB probes on admin port; debuggable HTTP semantics.

**Negative:** UDP/QUIC may be blocked on some networks; heavier than raw TCP; extra admin port + firewall rule; `h3` ecosystem pre-1.0.

## Related

- [client-and-routing.md](client-and-routing.md)
- [security.md](security.md)
- [certificates.md](certificates.md)
- [protocol.md](../protocol.md)
