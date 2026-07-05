# ADR 019: Cluster actor routing — round-robin + keyed send

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Open question **#5**: how `registry.cluster("workers")` picks an instance when multiple workers exist (primarily **dev multi-worker**; production = 1 worker per VPS per [ADR 014](014-one-worker-per-vps.md)).

User chose **both**: round-robin default + consistent hash for keyed messages.

## Decision

### Default: round-robin

`ClusterRef::send(msg)` delivers to the **next** instance in the pool (cluster-wide directory), cycling evenly.

```rust
registry.cluster("workers")?.send(WorkerMsg::Process(id)).await?;
```

- Simple, even load
- No stickiness guarantee

### Optional: consistent hash on key

`ClusterRef::send_keyed(key, msg)` routes to `hash(key) % instances` — stable while the instance set is unchanged.

```rust
registry.cluster("workers")?.send_keyed(order_id, WorkerMsg::Process(order_id)).await?;
```

- Same `order_id` → same worker (when that worker exists)
- Better for in-memory workflow state on the actor
- Uneven load if keys skew — user’s responsibility

### API

```rust
impl ClusterRef {
    pub async fn send<M: Serialize + Send>(&self, msg: M) -> Result<(), SendError>;
    pub async fn send_keyed<K: Hash, M: Serialize + Send>(
        &self,
        key: K,
        msg: M,
    ) -> Result<(), SendError>;
    pub async fn ask<M, R>(&self, msg: M) -> Result<R, AskError>;
    pub async fn ask_keyed<K: Hash, M, R>(&self, key: K, msg: M) -> Result<R, AskError>;
}
```

### Hash function

**Default:** `std::collections::hash_map::DefaultHasher` over `key` + stable cluster name salt.

Document: remapping when instances added/removed (consistent hash ring behavior lite — minimal v1: modulo; full ring deferred).

### Production note

With [ADR 014](014-one-worker-per-vps.md), production has **one instance per node** — round-robin spreads across VPSes; `send_keyed` still useful to pin work to a specific node’s worker when multiple VPSes exist.

## Rejected

| Option | Why not alone |
|--------|----------------|
| Round-robin only | No sticky workflows |
| Consistent hash only | Forces key on every send; worse ergo for stateless fan-out |

## Related

- [013-cross-node-actors.md](013-cross-node-actors.md)
- [014-one-worker-per-vps.md](014-one-worker-per-vps.md)
