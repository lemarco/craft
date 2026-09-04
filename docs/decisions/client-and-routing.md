# Client API & routing

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-08-28 — merged client-api, client-routing, cluster-routing, read-consistency

## Context

Trembita is **Rust-native, no gRPC**. Clients may connect to any node; only the leader appends to the log. Actor messages need cluster-wide routing. Reads of authoritative state must define a consistency level.

## Client API — no gRPC

Layered client API sharing **`trembita-proto` message types**:

| Layer | Transport | Audience |
|-------|-----------|----------|
| **In-process** | `ractor` message passing | Tests, embedded clusters |
| **Remote** | HTTP/3 + `postcard` ([wire-protocol](wire-protocol.md)) | External Rust clients, CLI |

```rust
// In-process
pub struct ClientHandle { /* ... */ }
impl ClientHandle {
    pub async fn propose(&self, cmd: impl Into<ClientCommand>) -> Result<ClientResponse, ClientError>;
    pub async fn query(&self, q: impl Into<ClientQuery>) -> Result<ClientResponse, ClientError>;
}

// Remote — followers forward to leader
pub struct RemoteClient { /* quinn::Endpoint, rustls::ClientConfig */ }
```

HTTP mapping:

```
POST /raft/v1/client/wire
Content-Type: application/x-postcard
Body: postcard(ClientRequest)
200 → postcard(ClientResponse)
```

Wire types in `trembita-proto/src/client.rs`: `ClientRequest { Propose, Query }`, `ClientResponse { Ok, NotLeader, Error }`.

**Rejected:** gRPC/tonic, framed TCP, tarpc.

Crates: `trembita-client`, `trembita-net`, `trembita-proto`.

## Client routing — transparent forward

Any node accepts client requests. If **not** the leader, it **forwards** to the current leader and **returns the leader's response**. Clients do not need leader discovery for normal operation.

```
Client → Follower → Leader → RaftCore → response proxied back
```

- Leader address from Raft state (`leader_id` + cluster config).
- No leader known → `503` with `ClientResponse::Err(ClientWireError::NoLeaderElected)`.
- `req_id` preserved; leader deduplicates via bounded cache.
- Combined client deadline covers follower + leader hop.
- Same rule for in-process `ClientHandle`.

**Rejected:** redirect-only (client retry), hybrid.

Metrics: `raft_client_forward_total`, `raft_client_forward_latency`.

## Cluster actor routing

When multiple worker instances exist (primarily dev multi-worker):

| Method | Behavior |
|--------|----------|
| `ClusterRef::send(msg)` | **Round-robin** across instances |
| `ClusterRef::send_keyed(key, msg)` | **Consistent hash** — stable while instance set unchanged |

```rust
impl ClusterRef {
    pub async fn send<M>(&self, msg: M) -> Result<(), SendError>;
    pub async fn send_keyed<K: Hash, M>(&self, key: K, msg: M) -> Result<(), SendError>;
    pub async fn ask<M, R>(&self, msg: M) -> Result<R, AskError>;
    pub async fn ask_keyed<K: Hash, M, R>(&self, key: K, msg: M) -> Result<R, AskError>;
}
```

Default hash: consistent hash ring (64 virtual nodes per member); see [actor-routing](actor-routing.md). Production: 1 worker per VPS — round-robin spreads across nodes; `send_keyed` pins work to a specific node's worker.

## Read consistency

Two read paths:

| Path | Consistency |
|------|-------------|
| `client.query` → `StateMachine::query` | **Linearizable** |
| `actor.ask` → user actor | Best-effort / local |

### Leader ReadIndex

For `ClientRequest::Query` on the leader:

1. Record `read_index = commit_index`.
2. Confirm leadership (heartbeat quorum / ReadIndex ack from majority).
3. Wait until `applied_index >= read_index`.
4. Execute `state_machine.query(q)`.

Read does **not** append to the Raft log.

### Lease reads (landed)

Leader may serve `query` without ReadIndex round-trip while holding a valid lease:

- Quorum ack of heartbeat grants lease lasting `election_timeout_min / 2` ticks.
- Surrendered on step-down; requires current-term committed entry.
- Fallback to full ReadIndex when lease invalid.

Implemented: `RaftNode::lease_read` in `trembita-core`; driver's `query` fast path.

### Follower reads (landed)

Non-leaders may serve `Query` locally (etcd-style):

1. Follower calls leader with `ReadIndexConfirm`.
2. Leader runs ReadIndex / lease confirm → `ReadIndexConfirmed { index, term }`.
3. Follower waits until `last_applied >= index`, then queries locally.

Writes (`Propose`) still forward to leader. Lease reads remain leader-only.

### Out of scope

Linearizable actor `ask` — use Raft `query` for authoritative SM reads.

### Linearizability testing

Layered verification per [testing-strategy](testing-strategy.md): unit (`trembita-core` ReadIndex FSM), property (proptest), deterministic sim + porcupine-style checker, E2E nightly (`e2e/linearizability.sh`). Stale-under-partition must surface as error/timeout, never wrong value.

## Consequences

**Positive:** One wire stack; any node address works behind LB; clear split: `query` = truth, `ask` = fast/local; correct reads across elections.

**Negative:** Extra hop latency on non-leader contacts; read load on leader (mitigated by follower reads); non-Rust clients must speak HTTP/3 + postcard.

## Related

- [wire-protocol.md](wire-protocol.md)
- [state-machine.md](state-machine.md)
- [cross-node-actors.md](cross-node-actors.md)
- [cluster-elasticity.md](cluster-elasticity.md)
- [protocol.md](../protocol.md)
