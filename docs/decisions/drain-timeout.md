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
| Environment | `TREMBITA_DRAIN_TIMEOUT` | `90s`, `2m`, `120` (seconds) |
| Reference `trembita-node` | via [`NodeConfig`](../crates/trembita-tools/src/node/config.rs) → [`AppConfig`](../crates/trembita/src/env.rs) (`EnvOverrides` when env vars were set) | Same env var |
| [`trembita-assembly`](../crates/trembita-assembly/src/builder/cluster/mod.rs) | `TrembitaClusterBuilder::drain_timeout` | Framework / integration / showcase only (not product API) |

Product apps read **`TREMBITA_DRAIN_TIMEOUT`** through [`TrembitaApp::from_env`](../../crates/trembita/src/app/runtime.rs) / [`from_config`](../../crates/trembita/src/app/runtime.rs) (default **60s** when unset).

### Behavior

1. `leave()` or migration starts → worker marked **draining** (no new `deliver` accepted).
2. Wait for in-flight handler tasks + optional Redis flush ([actor-state-redis](actor-state-redis.md)).
3. **Timeout elapsed** → force stop actor; migration proceeds with last Redis state.
4. Return error/warning in logs if drain incomplete (`DrainIncomplete` metric).

### Scope

- Applies per **actor instance** being migrated/stopped.
- Cluster-wide default via builder / `TREMBITA_DRAIN_TIMEOUT`.
- Per-group override via `ActorRegistry::set_group_drain_timeout` ([actor-routing](actor-routing.md)).

## Consequences

**Positive**

- Ops can tune per deployment (fast dev vs slow prod jobs)
- Works with Redis-backed state — force-stop less scary

**Negative**

- Must document timeout vs long-running work

## Related

- [cross-node-actors.md](cross-node-actors.md)
- [actor-state-redis.md](actor-state-redis.md)
- [cluster-elasticity.md#auto-spawn-on-join](cluster-elasticity.md#auto-spawn-on-join)
