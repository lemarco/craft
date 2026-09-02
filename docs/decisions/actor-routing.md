# Actor / routing UX

**Status:** Accepted  
**Date:** 2026-08-27

## Context

Cross-node actors shipped with modulo keyed routing, a fixed default drain timeout, eventual directory convergence (R3), and a clear split between linearizable Raft `query` and fast/local actor `ask`.

This record covers operator and application UX for sticky sessions, smoother scale events, and optional directory visibility — without changing consensus semantics.

## Decision

### Consistent hash ring (replaces `hash % N`)

Actor keyed routing (`ActorDirectory::pick_keyed`, local `PoolInner::pick_keyed`)
uses a **virtual-node ring** ([`trembita-actor/src/ring.rs`](../../crates/trembita-runtime/src/ring.rs)):
64 vnodes per member, clockwise successor from `hash(key)`, salted per group name.
Adding/removing an instance remaps roughly `1/N` of keys instead of almost all keys.

### Sticky session / lease

[`ActorSession`](../../crates/trembita-runtime/src/session.rs) pins casts/asks to a
specific [`ActorId`] until TTL expiry or the instance disappears.
Obtain via `ClusterRef::session_keyed` or `ActorSession::new`; deliver with
`ClusterMessaging::cast_session` / `ask_session`.

### Per-actor drain override

Cluster default remains **60s** ([`DEFAULT_DRAIN_TIMEOUT`](../../crates/trembita-runtime/src/registry.rs)).
[`ActorRegistry::set_group_drain_timeout`] overrides per group; [`stop_graceful`]
uses override when set, else the caller's default (facade:
[`TrembitaCluster::drain_timeout`](../../crates/trembita/src/cluster.rs),
[`TrembitaClusterBuilder::drain_timeout`](../../crates/trembita/src/builder.rs),
`TREMBITA_DRAIN_TIMEOUT` in `trembita-node`).

### Linearizable ask (optional)

Default `ask` stays fast/local. [`ClusterMessaging::ask_linearizable`] retries
directory visibility ([`DirectoryPolicy::ReadYourWrites`]) before delivery — for
rare cases that need a fresh directory view without paying Raft ReadIndex on actor
state.

Raft `query` remains the linearizable path for replicated state machine data.

### Directory strong consistency mode

[`DirectoryPolicy::ReadYourWrites`] enables brief retry on `NoTarget` after
spawn/scale (mitigates R3). Facade helper
[`TrembitaCluster::publish_directory_visible`] publishes then waits for local
visibility. Default remains eventual + periodic anti-entropy.

## Consequences

**Positive**

- Smoother scale-up/down for keyed workflows
- Workflow state on workers without Redis (session + ring)
- Ops can tune drain per heavy job group
- Callers can opt into read-your-writes without changing default latency

**Negative**

- Ring adds CPU vs modulo (bounded: 64 vnodes × members)
- Sessions can go stale after migration — callers must reopen or handle `NoTarget`

## Related

- [client-and-routing.md#cluster-actor-routing](client-and-routing.md#cluster-actor-routing)
- [drain-timeout.md](drain-timeout.md)
- [cross-node-actors.md](cross-node-actors.md)
- [client-and-routing.md#read-consistency](client-and-routing.md#read-consistency)
- [future-work-and-risks.md](future-work-and-risks.md) (R3)
