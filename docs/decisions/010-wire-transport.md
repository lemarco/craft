# ADR 010: Wire transport — HTTP/3 everywhere

**Status:** Accepted  
**Date:** 2026-07-05

## Context

[ADR 002](002-client-api.md) rejected gRPC in favor of Rust-native types. We considered framed TCP + `postcard` vs HTTP/3 for the client edge only. The user chose **HTTP/3 for all wire traffic** — Raft peer RPC, snapshots, and remote client API share one transport stack.

## Decision

**All network I/O uses HTTP/3 over QUIC.** No separate TCP stack for Raft internals.

| Traffic | Transport | Codec |
|---------|-----------|-------|
| Raft peer RPC (`RequestVote`, `AppendEntries`, `InstallSnapshot`, …) | HTTP/3 | `postcard` body |
| Remote client API (`Propose`, `Query`) | HTTP/3 | `postcard` body |
| In-process client (`ClientHandle`) | `ractor` | native types (no HTTP) |

**One QUIC listener per node** (default port configurable, e.g. `7443/udp`). Path-based routing distinguishes peer vs client traffic; both use the same serialization crate (`raft-proto`).

## HTTP routes (v1)

```
POST /raft/v1/peer/wire     # inter-node; mTLS with peer identity
POST /raft/v1/client/wire   # client API; TLS (mTLS optional — see ADR 006)
```

Request and response bodies: **`postcard`-encoded** messages from `raft-proto`.

```rust
// Request body
pub enum PeerWireMessage {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

// Client path uses ClientRequest in / ClientResponse out (ADR 002)
```

Each HTTP request is **one RPC round-trip** (request body in, response body out). Long-lived **QUIC connections** between peers amortize handshake cost; concurrent Raft messages use HTTP/3 stream multiplexing on the same connection.

### Response semantics (client path)

| HTTP | Meaning |
|------|---------|
| `200` + `application/x-postcard` | `ClientResponse` — handled or proxied ([ADR 003](003-client-routing.md)) |
| `503` / `504` | No leader or forward timeout |
| `4xx/5xx` | Transport or server error |

Peer path uses `200` with response message in body; Raft-level rejection encoded in response variant (e.g. stale term), not HTTP status.

## Rust stack

| Crate | Role |
|-------|------|
| `quinn` | QUIC transport |
| `h3`, `h3-quinn` | HTTP/3 |
| `rustls` | TLS 1.3 (required by QUIC) |
| `postcard` | Request/response bodies |
| `serde` | Type serialization |

`raft-net` owns the HTTP/3 server, peer connection pool, and outbound client. **No `tcp/` module.**

## Connection model

```
Node A                          Node B
  │                                │
  ├── QUIC conn (persistent) ──────┤
  │     ├── stream: AppendEntries
  │     ├── stream: RequestVote
  │     └── stream: client propose (if via B)
  └── h3 server listens :7443/udp
```

- **Peer pool:** one QUIC connection per remote `NodeId`; reconnect with backoff.
- **Clients:** `RemoteClient` opens QUIC to any member; caches leader address.
- **Backpressure:** HTTP/3 flow control + bounded outbound queues in `NetworkRouter`.

## Consequences

**Positive**

- Single transport story: one port, one TLS config, one codec
- Built-in encryption and multiplexing (addresses much of ADR 006)
- Debuggable with HTTP semantics (paths, status, headers)
- Still Rust-native — no protobuf or gRPC

**Negative**

- UDP/QUIC may be blocked on some networks (operational consideration)
- Heavier than raw TCP for tiny messages; must benchmark election/heartbeat latency
- `h3` ecosystem still pre-1.0 — pin versions, expect occasional API churn
- Couples client and consensus traffic on same listener — mitigated by path separation and stream multiplexing

**Mitigations**

- `raft-sim` uses in-memory transport implementing the same `Transport` trait (no real QUIC in unit sim)
- Dev profile: localhost self-signed certs; `insecure-dev` feature for tests only
- **Peer RPC uses a dedicated QUIC connection** separate from client/actor traffic ([ADR 027](027-future-work-and-risks.md) R2) — prevents heartbeat starvation under load
- Optional full traffic priorities later (HTTP/3 stream priorities / per-class rate limits)

## Alternatives rejected

| Option | Why rejected |
|--------|--------------|
| Framed TCP + postcard | User chose HTTP/3 for everything |
| HTTP/3 client only, TCP for peers | User chose HTTP/3 for everything |
| gRPC over HTTP/3 | User rejected gRPC |

## Related

- [002-client-api.md](002-client-api.md) — client types and `RemoteClient`
- [006-security.md](006-security.md) — mTLS policy (updated)
- [protocol.md](../protocol.md) — route and header details
