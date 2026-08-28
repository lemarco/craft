# Graceful drain timeout — configurable

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Medium topic: how long `/actor/migrate` and `cluster.leave()` wait for workers to drain in-flight messages before force-stop ([cross-node-actors](cross-node-actors.md)).

User chose **Option C — configurable** with a sensible default.

## Decision

### Default

**`60 seconds`** drain timeout cluster-wide.

### Configuration

| Source | Key | Example |
|--------|-----|---------|
| Builder | `.drain_timeout(Duration)` | `Duration::from_secs(120)` |
| Environment | `CRAFT_DRAIN_TIMEOUT` | `90s`, `2m`, `120` (seconds) |

Builder overrides env when both set.

```rust
CraftCluster::builder()
    .drain_timeout(Duration::from_secs(60)) // default if omitted
    .spawn()
    .await?;
```

### Behavior

1. `leave()` or migration starts → worker marked **draining** (no new `deliver` accepted).
2. Wait for in-flight handler tasks + optional Redis flush ([actor-state-redis](actor-state-redis.md)).
3. **Timeout elapsed** → force stop actor; migration proceeds with last Redis state.
4. Return error/warning in logs if drain incomplete (`DrainIncomplete` metric).

### Scope

- Applies per **actor instance** being migrated/stopped.
- Cluster-wide default via builder / `CRAFT_DRAIN_TIMEOUT`.
- Per-group override via `ActorRegistry::set_group_drain_timeout` ([actor-routing-tier3](actor-routing-tier3.md)).

## Consequences

**Positive**

- Ops can tune per deployment (fast dev vs slow prod jobs)
- Works with Redis-backed state — force-stop less scary

**Negative**

- Must document timeout vs long-running work

## Related

- [cross-node-actors.md](cross-node-actors.md)
- [actor-state-redis.md](actor-state-redis.md)
- [auto-spawn-on-join.md](auto-spawn-on-join.md)
