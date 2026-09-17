# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Design rationale: [decisions/](decisions/). Product vision: [decisions/product-scenarios.md](decisions/product-scenarios.md). Scenario guides: [scenarios/](scenarios/README.md).

When an item ships, remove its row here and update [status.md](status.md) (and the relevant ADR if needed).

---

## Open work

Optional integrations and maintenance — not blockers for [product scenarios](decisions/product-scenarios.md).

| Id | Item | Status | Notes |
|----|------|--------|-------|
| O-01 | `trembita-store-redis` maintenance | ongoing | Optional Redis `CapStateStore`; heavy tests in CI `run-heavy` |
| O-02 | PostgreSQL cap store hardening | ongoing | [`trembita-capstore-postgres`](../crates/trembita-capstore-postgres/README.md) — production CAS under concurrency (transactional) |

New feature epics: next **B-NN** (after B-27); add a row here with scenario + ADR links.

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md)
