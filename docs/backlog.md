# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Design rationale: [decisions/](decisions/). Product vision: [decisions/product-scenarios.md](decisions/product-scenarios.md). Scenario guides: [scenarios/](scenarios/README.md).

When an item ships, remove its row here and update [status.md](status.md) (and the relevant ADR if needed).

---

## Open work

Optional integrations and maintenance — not blockers for [product scenarios](decisions/product-scenarios.md).

| Id | Item | Status | Notes |
|----|------|--------|-------|
| O-01 | `trembita-store-redis` maintenance | ongoing | Keep as optional adapter |
| O-02 | PostgreSQL `ActorStateStore` | deferred | Only if external integration demand |
| O-04 | Governor finer signals (in-flight HTTP, consumer in-flight, optional `ConsumerTune::max_in_flight`) | deferred | [workload-governor § Future work](decisions/workload-governor.md#future-work) |
| O-06 | Automatic queue sharding under sustained enqueue pressure | deferred | [job-queue § Future work](decisions/job-queue.md#future-work); manual `job_queue_sharded` shipped |
| O-07 | Crate rename `trembita-actor-store` → `trembita-capstore` | deferred | Alias `trembita::capstore` only today; see [0.6.0 release](releases/0.6.0.md) |

New feature epics: next **B-NN** (after B-27); add a row here with scenario + ADR links.

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md)
