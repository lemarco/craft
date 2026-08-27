# Join RPC — dedicated `/cluster/join`

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Open question **#4**: how a joining VPS initiates cluster membership. [membership-early](membership-early.md) requires joint-consensus changes in the Raft log; this ADR defines the **operational handshake** before/at the log entry.

## Decision

**Option A — dedicated join route.**

```
POST /raft/v1/cluster/join
Content-Type: application/x-postcard
mTLS: joining node presents cert; target has --allow-join
```

**Request:** `postcard(JoinRequest)`

```rust
pub struct JoinRequest {
    pub node_id: NodeId,
    pub listen_addr: SocketAddr,
    pub protocol_version: u16,
    pub app_version: String,   // semver string for skew check
}
```

**Response:** `postcard(JoinResponse)`

```rust
pub enum JoinResponse {
    Ok { cluster_id: Uuid },
    Error { code: u16, message: String },
}
```

| HTTP | Meaning |
|------|---------|
| `200` | Join accepted; membership change **committed** in Raft log |
| `403` | `--allow-join` not set |
| `409` | Duplicate `NODE_ID`, version mismatch, or invalid cert |
| `503` | No leader / election in progress — retry |

### Flow

1. Joining node sends `JoinRequest` to `JOIN_ADDR`.
2. Receiver forwards to leader if needed ([client-routing](client-routing.md) pattern for operational RPCs — or join handled only on node that receives it, forwarded internally to leader).
3. Leader validates cert ↔ `node_id`, protocol/app version, `--allow-join`.
4. Leader proposes **joint-consensus ConfChange** (add node) → Raft log ([membership-early](membership-early.md)).
5. On commit → all nodes update config; `200 JoinResponse`.
6. [auto-spawn-on-join](auto-spawn-on-join.md): leader supervisor triggers auto workers on new node.

**Membership change is always in the Raft log** — the HTTP route is only the **entry point**, not a bypass of consensus.

### Rejected

| Option | Why |
|--------|-----|
| **B — `Propose(JoinNode)` only** | Chicken-and-egg: joiner not in cluster yet; awkward first contact |

## Related

- [membership-early.md](membership-early.md)
- [discovery.md](discovery.md)
- [protocol.md](../protocol.md)
