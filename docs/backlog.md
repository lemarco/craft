# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Shipped epic history: [archive/backlog-shipped.md](archive/backlog-shipped.md) (B-01…B-27), [archive/backlog-wave-b28-b32.md](archive/backlog-wave-b28-b32.md) (B-28…B-32), [archive/backlog-wave-b33-b41.md](archive/backlog-wave-b33-b41.md) (B-33…B-41). **Regression index:** [testing-coverage § B-28–B-32](testing-coverage.md#shipped-backlog-b-28b32) · [§ B-33–B-41](testing-coverage.md#shipped-backlog-b-33b41) · [§ B-42+](testing-coverage.md#shipped-backlog-b-42). Design rationale: [decisions/](decisions/).

When an item ships, remove its row here, update [status.md](status.md), and move planning text to `docs/archive/` if it is no longer needed in live docs.

---

## Open work

**Pre-1.0 policy:** breaking API/default changes are OK when they match product intent. Optional cap-store backends ship via `trembita` features — see [status.md](status.md).

**Priority hint:** P0 → P1 → P2 within this table (top first). Next epic id when adding work: **B-55**. **1.0 stabilization** (API freeze, Jepsen, semver) — **not** listed here; see [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md) when scheduling that program explicitly.

**Wave status:** product scale & ops epics **B-28 … B-54** (incl. doc hygiene **B-53**) are shipped — see [status § Product scale wave](status.md#product-scale-wave-b-28b32). Add new rows below when scheduling **B-55+**.

### Deferred decisions (ADR when epic starts)

Not scheduled as epics until product asks. Resolved items live in [status](status.md) / archives above.

| Topic | Options | Current lean |
|-------|---------|--------------|
| Ops cockpit UI | Dashboard page vs JSON only | **JSON introspect** shipped (B-43); add a dashboard page only if it beats raw `/introspect/*` for operators |
| Backup automation | Hot copy vs snapshot API vs trembita-ops only | **Manual DR pack** shipped (B-48); automated snapshot API only if ops demand it |
| 1.0 program | When to schedule | **Explicit decision only** — [public-api-1.0](decisions/public-api-1.0.md) · [jepsen-1.0](decisions/jepsen-1.0.md) |

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md) · [structural-limits](scenarios/structural-limits.md)
