# Wire protocol

**HTTP/3 over QUIC** for all network traffic ([ADR 010](decisions/010-wire-transport.md)). Bodies are **`postcard`-encoded** Rust types from `raft-proto` ([ADR 011](decisions/011-serialization.md)). No gRPC, no JSON on the hot path.

## Transport

| Property | Value |
|----------|-------|
| Protocol | HTTP/3 (QUIC, UDP) |
| Default port | `7443` (configurable) |
| TLS | Required (QUIC) — see [ADR 006](decisions/006-security.md) |
| Body codec | `postcard` |
| Content-Type | `application/x-postcard` |

## Routes

### Peer RPC (node ↔ node)

```
POST /raft/v1/peer/wire
Authorization: (mTLS client cert identifies NodeId)
Content-Type: application/x-postcard
```

**Request body:** `postcard(PeerWireMessage)`

```rust
pub enum PeerWireMessage {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResponse(InstallSnapshotResponse),
}
```

**Response:** `200 OK`, body = `postcard(PeerWireMessage)` (the reply variant).

Raft semantic errors (stale term, vote denied) are encoded **inside** the response message, not as HTTP error statuses.

### Client API (client → node)

```
POST /raft/v1/client/wire
Content-Type: application/x-postcard
```

**Request body:** `postcard(ClientRequest)`

```rust
pub enum ClientRequest {
    Propose { req_id: Uuid, payload: Vec<u8> },
    Query { req_id: Uuid, payload: Vec<u8> },
}
```

**Responses:**

| Status | When | Body |
|--------|------|------|
| `200` | Handled locally (leader) or proxied from leader ([ADR 003](decisions/003-client-routing.md)) | `postcard(ClientResponse)` |
| `503` | No leader elected / forward target unknown | `postcard(ClientResponse::Error)` |
| `504` | Forward to leader timed out | `postcard(ClientResponse::Error)` |
| `400` / `500` | Bad request / server fault | optional error body |

**Follower behavior:** if this node is not the leader, forward the same `ClientRequest` to the leader via `POST /raft/v1/client/wire` on the leader’s address and return the leader’s response. Clients do **not** need to retry on another node for normal leader changes.

```rust
pub enum ClientResponse {
    Ok { payload: Vec<u8> },
    NotLeader { leader_addr: Option<SocketAddr>, term: Term }, // reserved; not primary path
    Error { code: u16, message: String },
}
```

Typed command bytes in `payload` are defined by the user’s `StateMachine` ([ADR 001](decisions/001-state-machine.md)).

### Cluster join (node ↔ node)

```
POST /raft/v1/cluster/join
Content-Type: application/x-postcard
```

**Requires** target node started with `--allow-join` ([ADR 012](decisions/012-elastic-cluster.md)). Leader applies **joint-consensus membership change** via Raft log ([ADR 016](decisions/016-membership-early.md)).

| Status | When |
|--------|------|
| `200` | Join accepted; membership change initiated/completed |
| `403` | Join disabled (`--allow-join` not set) |
| `409` | Version mismatch ([ADR 020](decisions/020-join-version-skew.md)), duplicate `NODE_ID`, or invalid cert |

Request/response types: `JoinRequest` / `JoinResponse` in `craft-proto` ([ADR 017](decisions/017-join-rpc.md)).

### Actor delivery (cross-node, v1)

| Route | Purpose |
|-------|---------|
| `POST /raft/v1/actor/deliver` | Message / ask to actor mailbox |
| `POST /raft/v1/actor/spawn` | Remote spawn (`spawn_remote`, placement) |
| `POST /raft/v1/actor/migrate` | Snapshot transfer + respawn on target node |
| `POST /raft/v1/actor/register` | Directory publish / revoke |

See [ADR 013](decisions/013-cross-node-actors.md).

## Connections

- **Peers:** long-lived QUIC connection per remote node; concurrent RPCs on separate HTTP/3 streams.
- **Clients:** QUIC connection to **any** member; followers transparently forward to leader ([ADR 003](decisions/003-client-routing.md)).
- **Max body size:** default 16 MiB (configurable; snapshots may use chunked `InstallSnapshot` before single-frame limits).

## Versioning

```
Raft-Protocol-Version: 1
```

Added as an HTTP request header when breaking changes ship. v1 omits the header (implicit version 1).

## Dev vs production TLS

| Profile | Peer path | Client path (`/client/wire`) | In-process `ClientHandle` |
|---------|-----------|------------------------------|---------------------------|
| **dev** | Self-signed CA; `insecure-dev` | Same; skip verify in tests | No TLS |
| **production** | mTLS — cert maps to `NodeId` | **mTLS required** — client cert from cluster CA | No TLS |

User **browser HTTPS** (port 443) is separate — user’s own TLS, not craft `/client/wire`.

Details in [ADR 006](decisions/006-security.md).

## Related

- [decisions/010-wire-transport.md](decisions/010-wire-transport.md)
- [decisions/002-client-api.md](decisions/002-client-api.md)
- [architecture.md](architecture.md)
