# Client API — Rust-native, no gRPC

**Status:** Accepted  
**Date:** 2026-07-05  
**Amended:** 2026-07-05 — remote wire uses HTTP/3 ([wire-transport](wire-transport.md))

## Context

The initial blueprint proposed **tonic/gRPC** for external clients. gRPC adds protobuf codegen, a heavier dependency tree, and a contract oriented toward polyglot services—not ideal for a Rust-first distributed systems library.

We want something **Rust-native** while still supporting:

- In-process embedding (tests, single-process multi-node sim)
- Cross-process clients (CLI tools, separate apps talking to a cluster)

## Decision

**Do not use gRPC.** Use a layered client API:

| Layer | Transport | Audience |
|-------|-----------|----------|
| **L1 — In-process** | `ractor` message passing (`RpcReplyPort`, actor refs) | Tests, embedded clusters, same-process apps |
| **L2 — Remote** | **HTTP/3** + `postcard` body ([wire-transport](wire-transport.md)) | External Rust clients, CLI, other binaries |

Both layers share **`raft-proto` client message types** (`ClientRequest`, `ClientResponse`, …). Only the transport differs.

## Why this stack

**Unified with Raft wire ([wire-transport](wire-transport.md))**

- Peer RPC and client API both use HTTP/3 + `postcard` on the same node listener
- One TLS/QUIC configuration per node

**Rust-native**

- Plain Rust structs + `serde`/`postcard`
- No `.proto` files or tonic build pipeline
- Idiomatic API: `client.propose(cmd).await?`

## API shape (draft)

```rust
// crates/raft-client/src/lib.rs

/// In-process: holds an ActorRef to local RaftNodeActor
pub struct ClientHandle { /* ... */ }

impl ClientHandle {
    pub async fn propose(&self, cmd: impl Into<ClientCommand>) -> Result<ClientResponse, ClientError>;
    pub async fn query(&self, q: impl Into<ClientQuery>) -> Result<ClientResponse, ClientError>;
}

/// Remote: HTTP/3 client to any cluster member; followers forward to leader (client-routing)
pub struct RemoteClient {
    endpoint: quinn::Endpoint,
    tls: rustls::ClientConfig,
}

impl RemoteClient {
    pub async fn connect(addr: SocketAddr, tls: ClientTlsConfig) -> Result<Self, ClientError>;
    pub async fn propose(&mut self, cmd: impl Into<ClientCommand>) -> Result<ClientResponse, ClientError>;
}
```

HTTP mapping ([protocol.md](../protocol.md)):

```
POST /raft/v1/client/wire
Content-Type: application/x-postcard
Body: postcard(ClientRequest)

200 → postcard(ClientResponse)

503 / 504 → no leader or forward timeout (ClientResponse::Error)

Followers **forward** to leader; clients do not handle NotLeader for normal operation (see client-routing).
```

Wire messages in `crates/raft-proto/src/client.rs`:

```rust
pub enum ClientRequest {
    Propose { req_id: Uuid, payload: Vec<u8> },
    Query { req_id: Uuid, payload: Vec<u8> },
}

pub enum ClientResponse {
    Ok { payload: Vec<u8> },
    NotLeader { leader_addr: Option<SocketAddr>, term: Term },
    Error { code: u16, message: String },
}
```

Typed wrapper:

```rust
pub struct TypedClient<M: StateMachine> { inner: RemoteClient, _m: PhantomData<M> }

impl<M: StateMachine> TypedClient<M> {
    pub async fn propose(&mut self, cmd: M::Command) -> Result<(), M::Error> { /* encode/decode */ }
}
```

## Alternatives considered

| Option | Verdict |
|--------|---------|
| **gRPC (tonic)** | Rejected |
| **Framed TCP + postcard** | Rejected — user chose HTTP/3 everywhere ([wire-transport](wire-transport.md)) |
| **HTTP/3 client only, TCP for peers** | Rejected — same |
| **tarpc** | Rejected |

## Consequences

- **Positive:** One wire stack; TLS by default via QUIC
- **Positive:** Strong typing end-to-end for Rust clients
- **Negative:** Non-Rust clients must speak HTTP/3 + postcard (document in `protocol.md`)
- **Negative:** Heavier deps (`quinn`, `h3`, `rustls`) than minimal TCP

## Crate layout

| Crate | Role |
|-------|------|
| `raft-client` | `ClientHandle`, `RemoteClient`, `TypedClient` |
| `raft-net` | HTTP/3 server, peer pool, `Transport` impl |
| `raft-proto` | `ClientRequest`, `ClientResponse`, peer wire enums |

## Related

- [wire-transport.md](wire-transport.md) — HTTP/3 for peer RPC too
- [client-routing.md](client-routing.md) — transparent forward from followers
- [security.md](security.md) — mTLS for peer path
