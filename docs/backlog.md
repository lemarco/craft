# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Shipped epic history: [archive/backlog-shipped.md](archive/backlog-shipped.md) (B-01…B-27), [archive/backlog-wave-b28-b32.md](archive/backlog-wave-b28-b32.md). Design rationale: [decisions/](decisions/).

When an item ships, remove its row here, update [status.md](status.md), and move planning text to `docs/archive/` if it is no longer needed in live docs.

---

## Open work

**Pre-1.0 policy:** breaking API/default changes are OK when they match product intent. Optional cap-store backends ship via `trembita` features — see [status.md](status.md).

**Priority hint:** P0 → P1 → P2 within this table (top first). Next epic id after B-41 ships: **B-42**. **1.0 stabilization** — out of active backlog; see [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md).

| Id | Pri | Item | Status | Notes |
|----|-----|------|--------|-------|
### Deferred decisions (ADR when epic starts)

| Topic | Options | Current lean |
|-------|---------|--------------|
| Coordination profile | Env-only vs `TrembitaConfigure` | **Shipped B-37** — both via [`CoordinationGrowthPreset`](../crates/trembita/src/configure.rs) |

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md) · [structural-limits](scenarios/structural-limits.md)
