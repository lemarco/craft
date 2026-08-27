# Join version skew — hard reject

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Medium open question **#1**: when a node calls `POST /raft/v1/cluster/join` ([join-rpc](join-rpc.md)), how to handle `protocol_version` and `app_version` mismatches.

User chose **hard reject** (not warn-only or configurable lax mode).

## Decision

**Reject join on any version mismatch** — fail closed with HTTP **`409 Conflict`**.

### Checks (all on leader before ConfChange)

| Field | Source | Rule |
|-------|--------|------|
| **`protocol_version`** | `JoinRequest` + optional header `Raft-Protocol-Version` | Must fall in **`[MIN_COMPATIBLE_PROTOCOL_VERSION, PROTOCOL_VERSION]`** (rolling N/N−1 wire upgrades). Outside the band → `409` |
| **`app_version`** | `JoinRequest` (user app semver string) | Must **exactly equal** cluster’s committed app version (unchanged — mixed app semver still rejected) |

Cluster app version = semver reported by **existing members** (must be unanimous among live nodes; leader verifies via peer metadata or configured `CRAFT_APP_VERSION` env).

### Response

```rust
JoinResponse::Error {
    code: VERSION_MISMATCH,  // u16 constant
    message: "protocol 1 required; app 2.3.1 required, got 2.3.0",
}
```

HTTP `409`; no membership change proposed.

### Rationale

- Mixed protocol = wire incompatibility ([serialization](serialization.md)).
- Mixed app semver = mixed `StateMachine` / actor behavior → subtle corruption.
- Clear ops: **upgrade all nodes to the same version**, then add new VPS.

### Rolling upgrades

**Wire (protocol):** during a rolling deploy, nodes may temporarily run adjacent
protocol versions. Join and every framed request accept any version in
`[MIN_COMPATIBLE_PROTOCOL_VERSION .. PROTOCOL_VERSION]` (see
[`craft_proto::protocol_version_compatible`](../../crates/craft-proto/src/lib.rs)).
When bumping `PROTOCOL_VERSION`, raise the minimum only after the fleet has
drained the old wire.

**App semver:** still **exact match** required — upgrade all nodes to the same
`app_version` before adding a joiner with a different state machine build.

1. Upgrade existing nodes to the new **app** version (same semver everywhere).
2. Roll nodes one at a time; adjacent **protocol** versions may coexist.
3. Deploy new VPS with matching `app_version` and a compatible `protocol_version`.
4. Join.

### Development

`insecure-dev` may set `JoinVersionPolicy::AllowAny` for local sim only — not available in release builds.

## Alternatives rejected

| Option | Why |
|--------|--------|
| Warn only | Mixed cluster risk |
| Same-major only | User chose strict hard reject |
| Configurable default lax | User chose hard reject |

## Related

- [join-rpc.md](join-rpc.md)
- [membership-early.md](membership-early.md)
- [serialization.md](serialization.md)
