# Wire protocol

**HTTP/3 over QUIC** for all network traffic ([wire-protocol](decisions/wire-protocol.md)). Bodies are **`postcard`-encoded** Rust types from `raft-proto`. No gRPC, no JSON on the hot path.

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
| `200` | Handled locally (leader) or proxied from leader ([client-and-routing](decisions/client-and-routing.md)) | `postcard(ClientResponse)` |
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

**Requires** target node started with `--allow-join` ([cluster-elasticity](decisions/cluster-elasticity.md)). Leader applies **joint-consensus membership change** via Raft log ([cluster-membership](decisions/cluster-membership.md)).

| Status | When |
|--------|------|
| `200` | Join accepted; membership change initiated/completed |
| `403` | Join disabled (`--allow-join` not set) |
| `409` | Version mismatch ([cluster-membership](decisions/cluster-membership.md#version-skew--hard-reject)), duplicate `NODE_ID`, or invalid cert |

Request/response types: `JoinRequest` / `JoinResponse` in `crafty-proto` ([cluster-membership](decisions/cluster-membership.md#join-rpc)).

### Cluster leave (node ↔ node)

```
POST /raft/v1/cluster/leave
Content-Type: application/x-postcard
```

**Requires** target node started with `--allow-leave`. The leader applies a **joint-consensus membership change** removing `LeaveRequest.node_id` from Meta-Raft (or group 0 in single-group mode) ([cluster-membership](decisions/cluster-membership.md)); per-group sync removes the node from shard groups.

Request/response types: `LeaveRequest` / `LeaveResponse` in `crafty-proto` ([cluster-membership](decisions/cluster-membership.md#leave-rpc)).

### Cluster catalog add (multi-Raft, node ↔ node)

```
POST /raft/v1/cluster/catalog/add
Content-Type: application/x-postcard
```

**Multi-Raft only.** The Meta-Raft leader appends a [`CatalogCommand::AddGroups`](../../crates/crafty-proto/src/catalog.rs) entry to the Meta-Raft log (not the user state machine). All nodes replay the entry, update the in-memory catalog, extend keyed routing, and rebalance to adopt new groups ([multi-raft](decisions/multi-raft.md)).

Request/response types: `CatalogAddRequest` / `CatalogAddResponse` in `crafty-proto`. Facade: `CraftyCluster::add_raft_groups(count)`.

### Actor delivery (cross-node, v1)

| Route | Purpose |
|-------|---------|
| `POST /raft/v1/actor/deliver` | Message / ask to actor mailbox |
| `POST /raft/v1/actor/spawn` | Remote spawn (`spawn_remote`, placement) |
| `POST /raft/v1/actor/scale` | Forward a cluster-wide scale to the leader ([cluster-elasticity](decisions/cluster-elasticity.md)) |
| `POST /raft/v1/actor/migrate` | Snapshot transfer + respawn on target node |
| `POST /raft/v1/actor/stop` | Stop a group on a target node for a planned scale-down / removal |
| `POST /raft/v1/actor/register` | Directory publish / revoke |

See [cross-node-actors](decisions/cross-node-actors.md).

### Job queue (cross-node, tier C)

Leader-gated durable backlog ([job-queue](decisions/job-queue.md)). Mutations run on the **Raft leader**; followers **forward** client routes to the leader. After each leader mutation, `QueueReplicateOp` batches replicate to every other **reachable voter** in parallel; the client receives success only once all peers ack.

**Authorization**

| Route | Caller identity |
|-------|-----------------|
| `enqueue`, `lease`, `ack`, `nack`, `metrics` | Any cluster member (mTLS peer); followers forward to leader |
| `replicate` | **Leader only** — transport must tag the caller (`LocalTransport`, QUIC mTLS peer id); followers reject if `from != current Raft leader` |

```
POST /raft/v1/queue/enqueue
POST /raft/v1/queue/lease
POST /raft/v1/queue/ack
POST /raft/v1/queue/nack
POST /raft/v1/queue/metrics
POST /raft/v1/queue/replicate   # leader → voter sync only
Content-Type: application/x-postcard
```

| Route | Request | Response | Notes |
|-------|---------|----------|-------|
| `.../enqueue` | `QueueEnqueueRequest` | `QueueEnqueueReply { job_id, error }` | Optional `priority`, `not_before_ms`, `dedup_key`, `shard_key` |
| `.../lease` | `QueueLeaseRequest` | `QueueLeaseReply { jobs, error }` | Worker identified by `worker_node` + `worker_instance` |
| `.../ack` | `QueueAckRequest` | `QueueAckReply { error }` | Completes a lease |
| `.../nack` | `QueueNackRequest` | `QueueNackReply { error }` | Requeues immediately |
| `.../metrics` | `QueueMetricsRequest` | `QueueMetricsReply` | `pending`, `leased`, `oldest_pending_age_ms` for autoscale |
| `.../replicate` | `QueueReplicateRequest` | `QueueReplicateReply { error }` | Idempotent `QueueReplicateOp` batch; leader-authenticated |

### Actor workflow store (tier — workflow keys)

Leader-gated durable KV for stateful actors ([actor-state-store](decisions/actor-state-store.md)). Same leader-forward + voter-replicate pattern as the job queue. Default file: `{data_dir}/actor-store.redb`.

```
POST /raft/v1/actor-store/set
POST /raft/v1/actor-store/delete
POST /raft/v1/actor-store/compare-and-set
POST /raft/v1/actor-store/replicate   # leader → voter sync only
Content-Type: application/x-postcard
```

| Route | Request | Response | Notes |
|-------|---------|----------|-------|
| `.../set` | `StoreSetRequest` | `StoreSetReply { error }` | Optional `ttl_secs` |
| `.../delete` | `StoreDeleteRequest` | `StoreDeleteReply { error }` | Idempotent |
| `.../compare-and-set` | `StoreCompareAndSetRequest` | `StoreCompareAndSetReply { applied, error }` | Optimistic concurrency |
| `.../replicate` | `StoreReplicateRequest` | `StoreReplicateReply { error }` | Idempotent `StoreReplicateOp` batch; leader-authenticated |

Local reads use `ClusterActorStateStore::get` on the voter's redb file (no RPC).

Types live in `crafty-proto` (`queue.rs`). Facade client: [`ClusterJobQueue`](../../crates/crafty-actor/src/queue_service.rs) via [`CraftyCluster::job_queue`](../../crates/crafty/src/cluster.rs).

### Actor mailbox spool (tier B durability)

Optional write-ahead **`MailboxSpool`** (`RedbMailboxSpool` at `{data_dir}/mailbox-spool.redb`) for cross-node [`/actor/deliver`](decisions/cross-node-actors.md):

| Direction | Behavior |
|-----------|----------|
| **Outbox** | Envelope persisted before send; removed after peer acks delivery |
| **Inbox** | Envelope persisted before local mailbox enqueue; removed after accept |
| **Recovery** | Background drainer + startup replay of pending rows |

Enable via [`CraftyClusterBuilder::durable_mailbox`](../../crates/crafty/src/builder.rs)(`true`) with [`data_dir`](../../crates/crafty/src/builder.rs).

## Connections

- **Peers:** long-lived QUIC connection per remote node; concurrent RPCs on separate HTTP/3 streams.
- **Clients:** QUIC connection to **any** member; followers transparently forward to leader ([client-and-routing](decisions/client-and-routing.md)).
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

User **browser HTTPS** (port 443) is separate — user’s own TLS, not crafty `/client/wire`.

Details in [security](decisions/security.md).

## Related

- [decisions/wire-protocol.md](decisions/wire-protocol.md)
- [decisions/client-and-routing.md](decisions/client-and-routing.md)
- [architecture.md](architecture.md)
