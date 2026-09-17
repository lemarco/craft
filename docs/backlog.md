# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Shipped epic history: [archive/backlog-shipped.md](archive/backlog-shipped.md) (B-01…B-27), [archive/backlog-wave-b28-b32.md](archive/backlog-wave-b28-b32.md) (B-28…B-32), [archive/backlog-wave-b33-b41.md](archive/backlog-wave-b33-b41.md) (B-33…B-41). **Regression index:** [testing-coverage § B-28–B-32](testing-coverage.md#shipped-backlog-b-28b32) · [§ B-33–B-41](testing-coverage.md#shipped-backlog-b-33b41) · [§ B-42+](testing-coverage.md#shipped-backlog-b-42). Design rationale: [decisions/](decisions/).

When an item ships, remove its row here, update [status.md](status.md), and move planning text to `docs/archive/` if it is no longer needed in live docs.

---

## Open work

**Pre-1.0 policy:** breaking API/default changes are OK when they match product intent. Optional cap-store backends ship via `trembita` features — see [status.md](status.md).

**Priority hint:** P0 → P1 → P2 within this table (top first). Next epic id when adding work: **B-55**. **1.0 stabilization** (API freeze, Jepsen, semver) — **not** listed here; see [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md) when scheduling that program explicitly.

_No open P0–P2 epics in this table (2026-09-17). Add rows here when scheduling new work._

### Deferred decisions (ADR when epic starts)

| Topic | Options | Current lean |
|-------|---------|--------------|
| Coordination profile | Env-only vs `TrembitaConfigure` | **Shipped B-37** — both via [`CoordinationGrowthPreset`](../crates/trembita/src/configure.rs) |
| Closed-loop ceilings | Env-only vs configure API | **Shipped B-44** — `TREMBITA_COORDINATION_MAX_*` + [`TrembitaConfigure`](../crates/trembita/src/configure.rs) |
| Ops cockpit UI | Dashboard page vs JSON only | **JSON introspect first** (B-43); reuse [trembita-dashboard](../crates/trembita-dashboard/) only if a page adds clear value |
| Backup scope | Hot copy vs snapshot API | **Document supported manual procedure first** (B-48); automated snapshot only if ops demand it |
| CI upgrade binary | `trembita-ops` vs `trembita-cli` release | **Shipped B-54** — [`trembita-ops upgrade run`](../crates/trembita-tools/src/bin/ops.rs) |
| Upgrade without self-update API | SSH/systemd manual roll | **Shipped B-45** — [deploy/rolling-upgrade-systemd.md](../deploy/rolling-upgrade-systemd.md); operator **fails fast** on 404 `/cluster/upgrade` |
| 1.0 program | When to schedule | **Explicit decision only** — not part of the B-42…B-54 shipped wave; track in [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md) |

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md) · [structural-limits](scenarios/structural-limits.md)
