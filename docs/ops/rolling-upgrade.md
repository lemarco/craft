# Rolling upgrade runbook

How to upgrade a crafty cluster without downtime. Distinguishes **wire protocol**
(rolling N/N−1 band) from **application semver** (exact match required).

## Two version axes

| Axis | Field | Rolling policy |
|------|-------|----------------|
| **Wire protocol** | `protocol_version` / `Raft-Protocol-Version` header | **Band** `[MIN_COMPATIBLE_PROTOCOL_VERSION .. PROTOCOL_VERSION]` — adjacent releases may coexist during staggered restarts |
| **App state machine** | `app_version` in join RPC | **Exact match** — mixed semver risks incompatible `StateMachine` / actor behaviour |

See [cluster-membership.md#version-skew--hard-reject](../decisions/cluster-membership.md#version-skew--hard-reject).

## Wire-only rolling restart (same app semver)

Use when the release bumps `PROTOCOL_VERSION` but not your app build.

1. Ensure every running node reports the **same** `app_version` (your semver string).
2. Upgrade nodes **one at a time** to the new binary (new wire, same app).
3. Each node accepts peers in the compatibility band; old and new wire may coexist briefly.
4. After the fleet is on the new binary, raise `MIN_COMPATIBLE_PROTOCOL_VERSION` only in a **subsequent** release once no N−1 nodes remain.

Verify with `./scripts/test-with-log.sh -p crafty-net --test protocol_compat`.

## App semver upgrade (state machine change)

When your `StateMachine`, actors, or command encoding changes:

1. **Stop** adding nodes.
2. Upgrade **all** existing members to the **same** new app semver (rolling wire OK, app must match before join completes).
3. Configure `CRAFTY_APP_VERSION` (or equivalent builder field) identically on every node.
4. Only then deploy new VPS joiners with the matching semver.

Join rejects with `409 VERSION_MISMATCH` if either axis fails.

## Why app_version stays strict

Mixed app versions can produce divergent apply results on the same committed log
entries — silent corruption. Wire compatibility only guarantees framing and RPC
shape, not application semantics.

## Checklist

- [ ] Same `app_version` on all nodes before expanding membership
- [ ] Protocol in band: `MIN_COMPATIBLE .. PROTOCOL_VERSION`
- [ ] Snapshot / `crafty-ops backup export` before risky upgrades
- [ ] Post-upgrade: `/ready`, sample propose/query, `e2e/run.sh` or nightly linearizability gate

## Automated self-update (no external orchestrator)

For a **leader-coordinated**, **self-downloading** rolling upgrade (StateMachine grants
one node at a time; binary replaces itself and exits; systemd restarts), see
[upgrade-coordinator.md](../decisions/upgrade-coordinator.md).

That pattern automates this runbook. It does **not** relax `app_version` rules below.

## Related

- [upgrade-coordinator.md](../decisions/upgrade-coordinator.md) — proposed reference pattern
- [cluster-membership.md#version-skew--hard-reject](../decisions/cluster-membership.md#version-skew--hard-reject)
- [multi-raft.md#production-reliability](../decisions/multi-raft.md#production-reliability)
- [docs/ops/backup-restore.md](backup-restore.md)
