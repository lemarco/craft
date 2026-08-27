# Wire protocol

**HTTP/3 over QUIC** for all network traffic ([wire-transport](decisions/wire-transport.md)). Bodies are **`postcard`-encoded** Rust types from `raft-proto` ([serialization](decisions/serialization.md)). No gRPC, no JSON on the hot path.

## Transport

| Property | Value |
|----------|-------|
| Protocol | HTTP/3 (QUIC, UDP) |
| Default port | `7443` (configurable) |
| TLS | Required (QUIC) — see [security](decisions/security.md) |
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
| `200` | Handled locally (leader) or proxied from leader ([client-routing](decisions/client-routing.md)) | `postcard(ClientResponse)` |
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

Typed command bytes in `payload` are defined by the user’s `StateMachine` ([state-machine](decisions/state-machine.md)).

### Cluster join (node ↔ node)

```
POST /raft/v1/cluster/join
Content-Type: application/x-postcard
```

**Requires** target node started with `--allow-join` ([elastic-cluster](decisions/elastic-cluster.md)). Leader applies **joint-consensus membership change** via Raft log ([membership-early](decisions/membership-early.md)).

| Status | When |
|--------|------|
| `200` | Join accepted; membership change initiated/completed |
| `403` | Join disabled (`--allow-join` not set) |
| `409` | Version mismatch ([join-version-skew](decisions/join-version-skew.md)), duplicate `NODE_ID`, or invalid cert |

Request/response types: `JoinRequest` / `JoinResponse` in `craft-proto` ([join-rpc](decisions/join-rpc.md)).

### Cluster leave (node ↔ node)

```
POST /raft/v1/cluster/leave
Content-Type: application/x-postcard
```

**Requires** target node started with `--allow-leave`. The leader applies a **joint-consensus membership change** removing `LeaveRequest.node_id` from group 0 ([per-group-raft-membership](decisions/per-group-raft-membership.md)); per-group sync removes the node from shard groups.

Request/response types: `LeaveRequest` / `LeaveResponse` in `craft-proto` ([leave-rpc](decisions/leave-rpc.md)).

### Cluster catalog add (multi-Raft, node ↔ node)

```
POST /raft/v1/cluster/catalog/add
Content-Type: application/x-postcard
```

**Multi-Raft only.** The group 0 leader appends a [`CatalogCommand::AddGroups`](../../crates/craft-proto/src/catalog.rs) entry to the group 0 Raft log (not the user state machine). All nodes replay the entry, update the in-memory catalog, extend keyed routing, and rebalance to adopt new groups ([tier2-multi-raft-architecture](decisions/tier2-multi-raft-architecture.md)).

Request/response types: `CatalogAddRequest` / `CatalogAddResponse` in `craft-proto`. Facade: `CraftCluster::add_raft_groups(count)`.

### Actor delivery (cross-node, v1)

| Route | Purpose |
|-------|---------|
| `POST /raft/v1/actor/deliver` | Message / ask to actor mailbox |
| `POST /raft/v1/actor/spawn` | Remote spawn (`spawn_remote`, placement) |
| `POST /raft/v1/actor/scale` | Forward a cluster-wide scale to the leader ([supervisor-leader](decisions/supervisor-leader.md)) |
| `POST /raft/v1/actor/migrate` | Snapshot transfer + respawn on target node |
| `POST /raft/v1/actor/stop` | Stop a group on a target node for a planned scale-down / removal |
| `POST /raft/v1/actor/register` | Directory publish / revoke |

See [cross-node-actors](decisions/cross-node-actors.md).

## Connections

- **Peers:** long-lived QUIC connection per remote node; concurrent RPCs on separate HTTP/3 streams.
- **Clients:** QUIC connection to **any** member; followers transparently forward to leader ([client-routing](decisions/client-routing.md)).
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

Details in [security](decisions/security.md).

## Related

- [decisions/wire-transport.md](decisions/wire-transport.md)
- [decisions/client-api.md](decisions/client-api.md)
- [architecture.md](architecture.md)
