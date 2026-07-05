# ADR 020: Join version skew — hard reject

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Medium open question **#1**: when a node calls `POST /raft/v1/cluster/join` ([ADR 017](017-join-rpc.md)), how to handle `protocol_version` and `app_version` mismatches.

User chose **hard reject** (not warn-only or configurable lax mode).

## Decision

**Reject join on any version mismatch** — fail closed with HTTP **`409 Conflict`**.

### Checks (all on leader before ConfChange)

| Field | Source | Rule |
|-------|--------|------|
| **`protocol_version`** | `JoinRequest` + optional header `Raft-Protocol-Version` | Must **exactly equal** cluster’s supported protocol version (v1 → `1`) |
| **`app_version`** | `JoinRequest` (user app semver string) | Must **exactly equal** cluster’s committed app version |

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

- Mixed protocol = wire incompatibility ([ADR 011](011-serialization.md)).
- Mixed app semver = mixed `StateMachine` / actor behavior → subtle corruption.
- Clear ops: **upgrade all nodes to the same version**, then add new VPS.

### Rolling upgrades

1. Upgrade all existing nodes to new app version (same semver everywhere).
2. Deploy new VPS with **matching** `app_version`.
3. Join.

No partial mixed cluster in production.

### Development

`insecure-dev` may set `JoinVersionPolicy::AllowAny` for local sim only — not available in release builds.

## Alternatives rejected

| Option | Why |
|--------|--------|
| Warn only | Mixed cluster risk |
| Same-major only | User chose strict hard reject |
| Configurable default lax | User chose hard reject |

## Related

- [017-join-rpc.md](017-join-rpc.md)
- [016-membership-early.md](016-membership-early.md)
- [011-serialization.md](011-serialization.md)
