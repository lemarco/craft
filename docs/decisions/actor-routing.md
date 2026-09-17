# Actor / routing UX

**Status:** Accepted  
**Date:** 2026-08-27

## Context

Cross-node actors shipped with modulo keyed routing, a fixed default drain timeout, eventual directory convergence (R3), and a clear split between linearizable Raft `query` and fast/local actor `ask`.

This record covers operator and application UX for sticky sessions, smoother scale events, and optional directory visibility — without changing consensus semantics.

## Decision

### Consistent hash ring (replaces `hash % N`)

Actor keyed routing (`ActorDirectory::pick_keyed`, local `PoolInner::pick_keyed`)
uses a **virtual-node ring** ([`trembita-runtime/src/ring.rs`](../../crates/trembita-runtime/src/ring.rs)):
64 vnodes per member, clockwise successor from `hash(key)`, salted per group name.
Adding/removing an instance remaps roughly `1/N` of keys instead of almost all keys.

### Sticky session / lease

[`ActorSession`](../../crates/trembita-runtime/src/session.rs) pins casts/asks to a
specific [`ActorId`] until TTL expiry or the instance disappears.
Obtain via `ClusterRef::session_keyed` or `ActorSession::new`; deliver with
`ClusterMessaging::cast_session` / `ask_session`.

### Per-actor drain override

Cluster default remains **60s** ([`DEFAULT_DRAIN_TIMEOUT`](../../crates/trembita-runtime/src/registry/mod.rs)).
[`ActorRegistry::set_group_drain_timeout`] overrides per group; [`stop_graceful`]
uses override when set, else the caller's default (facade:
[`TrembitaCluster::drain_timeout`](../../crates/trembita/src/cluster.rs),
[`TREMBITA_DRAIN_TIMEOUT`](../../crates/trembita-assembly/src/env_config.rs) / in-crate [`TrembitaClusterBuilder::drain_timeout`](../../crates/trembita-assembly/src/builder/cluster/mod.rs),
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

### R3 visibility & sticky recovery (B-36)

The actor directory is **eventually consistent (R3)** — after join, scale, or **multi-Raft rebalance**, merged views can lag and cross-node deliver may return **`NoTarget`**. B-36 adds **operator surfaces** and **sticky session recovery** without changing the default eventual policy.

| Surface | Purpose |
|---------|---------|
| `GET /metrics` | `trembita_directory_merge_lag_epochs{node}` (gauge), `trembita_directory_deliver_no_target_total{group}` (counter deltas) — refreshed in [`assemble.rs`](../../crates/trembita-assembly/src/builder/cluster/assemble.rs) |
| [`TrembitaEvent::DirectoryDeliverNoTarget`](../../crates/trembita-dashboard/src/telemetry.rs) / `DirectoryMergeLag` | SSE / event sinks when lag or new `NoTarget` totals appear |
| **`GET /introspect/directory-r3`** | Same numbers as metrics + retry config ([`DirectoryR3Snapshot`](../../crates/trembita/src/app/directory_r3.rs)) |
| **`GET /introspect/ops-summary`** | **`directory_r3`** section ≡ `/introspect/directory-r3` ([B-43](../scenarios/capabilities.md#ops-cockpit-introspect-b-43)) |
| [`ActorSession::reopen_keyed`](../../crates/trembita-runtime/src/session.rs) / [`reopen_str`](../../crates/trembita-runtime/src/session.rs) | Re-pin sticky workers after migration (reuse live target when still registered) |
| [`TrembitaApp::reopen_session_str`](../../crates/trembita/src/app/runtime.rs) | Facade helper for app/gateway code |
| [`SessionHandle::reopen`](../../crates/trembita/src/gateway/session.rs) | Gateway cast/ask auto-reopen |

**`DirectoryR3Snapshot` JSON fields:** `directory_policy` (`read_your_writes` for product caps), `directory_retry_max_attempts`, `directory_retry_backoff_ms`, `directory_retry_boost_active`, `local_directory_epoch`, `merge_lag_epochs`, `deliver_no_target_totals` (map group → cumulative count since process start).

Product [`CapManifest`](../../crates/trembita/src/capability/manifest.rs) apps default to **ReadYourWrites** (**8 × 25 ms**). Assembly temporarily boosts to **24 × 40 ms** for **3 s** after multi-Raft group adopt/retire ([`boost_directory_retry_after_rebalance`](../../crates/trembita-runtime/src/messaging.rs)). Counters live in [`DirectoryDeliveryStats`](../../crates/trembita-runtime/src/directory_delivery.rs); merge lag math in [`ActorDirectory::merge_lag_epochs`](../../crates/trembita-runtime/src/directory.rs).

Runbook: [production-runbook § R3 directory](../ops/production-runbook.md#r3-directory-visibility-b-36) · cheat sheet: [structural-limits § R3](../scenarios/structural-limits.md#r3--actor-directory-is-eventually-consistent) · scenarios: [capabilities § B-36](../scenarios/capabilities.md#r3-directory-visibility-b-36).

#### Automated regression (B-36)

| Scenario | Test filter |
|----------|-------------|
| Per-group `NoTarget` totals + hook | `b36_no_target_totals_*`, `b36_on_no_target_*` (`trembita-runtime/directory_delivery.rs`) |
| Merge lag epoch table | `b36_merge_lag_scenarios_table` (`trembita-runtime/directory.rs`) |
| Sticky reopen (reuse / re-pick / empty group) | `b36_reopen_*` (`trembita-runtime/session.rs`) |
| HTTP body ≡ `directory_r3_snapshot()` | `b36_introspect_directory_r3_reports_ryw_defaults` |
| Ops route mounted + JSON fields | `b36_product_ops_only_exposes_directory_r3_route` (`tests/directory_r3_report.rs`) |
| Inline caps during `add_raft_groups` + bounded lag | `capability_inline_survives_raft_group_rebalance` (`integration/cap_rebalance.rs`) |

```bash
./scripts/test-fast.sh -p trembita-runtime --lib b36_
./scripts/test-fast.sh -p trembita --test directory_r3_report b36_
./scripts/test-fast.sh -p trembita --lib capability_inline_survives_raft_group_rebalance
```

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
