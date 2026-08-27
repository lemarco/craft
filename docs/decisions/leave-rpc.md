# Leave RPC — dedicated `/cluster/leave`

**Status:** Accepted  
**Date:** 2026-08-27

## Context

[ADR 033 — per-group Raft membership](per-group-raft-membership.md) Phase 2 landed per-group membership sync on group 0 join/leave facts. Join already had a public operational handshake ([join-rpc](join-rpc.md)); leave was only reachable via internal `propose_membership` in tests. Operators and the reference binary need the same explicit entry point as join.

## Decision

**Dedicated leave route — symmetric to join.**

```
POST /raft/v1/cluster/leave
Content-Type: application/x-postcard
mTLS: leaving node presents cert; target has --allow-leave
```

**Request:** `postcard(LeaveRequest)`

```rust
pub struct LeaveRequest {
    pub protocol_version: u32,
    pub node_id: NodeId,
}
```

**Response:** `postcard(LeaveResponse)`

```rust
pub enum LeaveResponse {
    Accepted { leader: NodeId, membership: Membership },
    Redirect { leader: Option<NodeId> },
    Rejected { reason: LeaveRejection },
}
```

| Rejection | When |
|-----------|------|
| `VersionSkew` | `protocol_version` mismatch ([join-version-skew](join-version-skew.md)) |
| `LeavesDisabled` | `--allow-leave` not set on the contacted node |
| `NotMember` | `node_id` not in committed voters |
| `LastMember` | Removing `node_id` would empty the voter set |
| `Other` | Overlapping membership change, etc. |

### Flow

1. Leaving node sends `LeaveRequest` to any live member (or uses [`CraftCluster::leave`](../../crates/craft/src/cluster.rs) which retries peers).
2. Receiver forwards to the group **0** leader if needed ([client-routing](client-routing.md) pattern — same as join; multi-Raft routes `ClusterLeave` to group 0 only).
3. Leader validates `--allow-leave`, membership, and protocol version.
4. Leader proposes **joint-consensus ConfChange** (remove `node_id`) → Raft log ([membership-early](membership-early.md)).
5. On commit → `LeaveResponse::Accepted` with updated membership.
6. [per-group-raft-membership](per-group-raft-membership.md): facts-refresher sync removes the node from shard groups on the next tick.

**Membership change is always in the Raft log** — the HTTP route is only the **entry point**, not a bypass of consensus.

### Facade API

| API | Role |
|-----|------|
| `CraftClusterBuilder::allow_leave(bool)` | Gate inbound leave RPC (default `false`) |
| `CraftCluster::request_leave(transport, contact)` | Low-level wire call (symmetric to `send_join_request`) |
| `CraftCluster::leave()` | Graceful self-removal: retries peers on the node's transport until accepted or timeout |

Actor migration before leave remains the caller's job ([cross-node-actors](cross-node-actors.md), [drain-timeout](drain-timeout.md)); `leave()` removes the node from Raft membership only.

### Reference binary

`craft-node` with `CRAFT_GRACEFUL_LEAVE=1` calls `CraftCluster::leave()` on `SIGINT` before shutdown.

### Rejected

| Option | Why |
|--------|-----|
| **Leave via `Propose(RemoveNode)` client write** | Joiner/leavers are not always full clients; operational RPC matches join |
| **Per-group leave RPC** | Group 0 is the cluster registry; shard groups follow ADR 033 sync |

## Related

- [join-rpc.md](join-rpc.md)
- [membership-early.md](membership-early.md)
- [per-group-raft-membership.md](per-group-raft-membership.md)
- [protocol.md](../protocol.md)
