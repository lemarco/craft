# External validation (Jepsen / Antithesis)

**Status:** Accepted (evaluation)  
**Date:** 2026-08-28  
**Backlog:** B-12a, B-12b

## Context

trembita already validates correctness via:

- Pure Raft FSM + property tests (`trembita-core`)
- Seeded deterministic simulation + linearizability checker (`trembita-sim`)
- Integration tests over `LocalNetwork` and real loopback QUIC
- Scheduled E2E (docker-compose, chaos, cert renew)
- Scenario soak harness (`benchmarks/` — B-10)

External tools (Jepsen, Antithesis) find **different** failure modes: JVM/process
isolation, real clock skew, OS-level partitions, and long-run state exploration.

## Scope evaluation (B-12a)

| Subsystem | Jepsen fit | Notes |
|-----------|------------|-------|
| Raft consensus (group 0) | **High** | Linearizable register SM maps cleanly to Jepsen models |
| Multi-Raft catalog / keyed routing | **Medium** | Multiple independent groups + migration; model complexity |
| Job queue | **Medium** | At-least-once + lease; needs queue-specific checker |
| Saga journal + resume | **Medium** | Depends on multi-group history; resume after kill is key |
| Actor directory / sessions | **Low–Medium** | Liveness-heavy; harder invariant spec |
| Actor workflow store | **Medium** | Voter-replicated KV; overlap with Raft tests |

**Recommendation:** A 1.0 Jepsen effort should **start with single-group Raft +
linearizable SM**, then add **one product scenario** (queue *or* saga), not all
four at once.

Antithesis-style exploration is complementary for **resume / restart** paths
already covered by scenario soaks (B-10).

## Go / no-go for 1.0 (B-12b)

| Criterion | Go | No-go |
|-----------|----|-------|
| Fast + nightly CI green for 4 weeks | Required | Block 1.0 tag |
| Scenario soaks (B-10) in scheduled `bench` job | Required | — |
| Known R1–R6 limits documented in [status.md](../status.md) | Required | — |
| Jepsen single-group Raft green | **Aspirational** | Does not block 0.x; blocks **marketing** "formally verified" claims |
| Full four-scenario Jepsen | **Non-goal** for 1.0 | Target 1.x+ if product demand |

**Decision:** Ship **1.0** when API + docs gates pass (B-11) and scenario soaks
are stable; treat Jepsen as a **follow-up epic** unless funding dedicates an engineer
for 2+ months.

## Related

- [testing-strategy.md](testing-strategy.md)
- [future-work-and-risks.md](future-work-and-risks.md)
