# Testing strategy

**Status:** Accepted  
**Date:** 2026-07-05

## Context

trembita is a distributed consensus + actor framework: the hardest bugs are **timing- and partition-dependent** (split brain, lost commits, stale reads, migration races). Traditional "spin up containers and poke it" testing is slow, flaky, and finds these bugs rarely and non-reproducibly.

Two design choices make a rigorous strategy cheap:

1. **`trembita-core` is a pure FSM** (`RaftInput → RaftOutput`, no I/O). It can be driven by a deterministic, seeded scheduler.
2. **`Transport` is a trait** ([wire-protocol](wire-protocol.md)). An in-memory implementation can inject delay/drop/partition without real sockets.

Together these let **deterministic simulation** be the primary bug-finder, with containers/E2E as a thin confidence layer.

## Decision

Adopt a **testing pyramid** with deterministic simulation at its core.

### Layers

| Layer | Scope | Tooling | CI lane |
|-------|-------|---------|---------|
| **Unit** | pure functions, per crate | `cargo-nextest` | fast |
| **Property** | Raft safety invariants (election safety, log matching, leader completeness, state-machine safety) | `proptest` | fast |
| **Compile-fail** | macro misuse rejected with good errors | `trybuild` | fast |
| **Deterministic simulation** ⭐ | whole cluster in one process, seeded/reproducible; partitions, delays, reorder, drops, join/leave, migrate, `scale_cluster` | **`trembita-sim`** (virtual clock + in-mem `Transport`) | fast |
| **Linearizability** | client-visible history is linearizable | `porcupine`-style checker over sim histories | fast |
| **Fuzz** | wire decoders (`postcard`) never panic / OOM | `cargo-fuzz` (libFuzzer) | nightly |
| **Integration** | N nodes, **one process**, real loopback QUIC + mTLS | tokio async tests | fast |
| **E2E** | real **processes/containers**, real network, real mTLS, chaos | `docker-compose` + `pumba`/`toxiproxy` | nightly |
| **External deps** | Redis `ActorStateStore` against real Redis | **`testcontainers-rs`** | fast |
| **Bench / soak** | throughput, latency, leak/soak over hours | `criterion` + long-run harness | nightly |
| **Concurrency (deferred)** | lock-free/atomics interleavings, if any introduced | `loom` | on-demand |

### Principles

- **Determinism first.** Every sim test takes a **seed**; failures print the seed and replay identically. The bug matrix (partition topologies × timing) lives here — fast and reproducible.
- **Containers are the thin top.** E2E validates the *real* wire (HTTP/3 QUIC), TLS handshake, cert loading, and process/OS boundaries — not consensus correctness. Keep E2E scenarios small: bootstrap, join, leave, migrate, one partition.
- **Test containers only for real external services** (Redis now; Postgres later if added). Not for trembita nodes in the fast lane.
- **Every fixed bug gets a regression test** at the lowest layer that reproduces it (usually a seeded sim case).
- **No `sleep`-based synchronization** in deterministic tests; advance the virtual clock.

### What each crate owns

| Crate | Primary layers |
|-------|----------------|
| `trembita-proto` | unit, fuzz (decode), property (roundtrip encode/decode) |
| `trembita-core` | unit, **property**, driven by `trembita-sim` |
| `trembita-storage` | unit, crash-recovery (kill mid-write, reopen) |
| `trembita-net` | unit, integration (loopback QUIC + mTLS) |
| `trembita-macros` | **trybuild** compile-pass/fail |
| `trembita-actor` | integration, driven by `trembita-sim` (migrate/scale/deliver) |
| `trembita-client` | integration (forward, leader-follow, retries) |
| `trembita-store-redis` | **testcontainers** Redis integration |
| `trembita-sim` | the harness itself + scenario suite |
| `trembita` / `trembita-node` | E2E (docker-compose + chaos) |

### CI mapping ([library-and-publishing](library-and-publishing.md))

- **Fast lane (every PR):** fmt, clippy `-D warnings`, nextest (unit/property/integration), trybuild, sim suite, Redis testcontainer, `publish --dry-run`, MSRV.
- **Nightly / release:** fuzz corpus run, full E2E chaos (docker-compose + pumba), criterion benches, soak.

### Aspirational

- Jepsen / Antithesis-style external validation before a **marketing** "formally
  verified" claim — see [jepsen-1.0.md](jepsen-1.0.md) for scope and go/no-go.

### Scenario soak (B-10, scheduled CI)

Long-run in-process harnesses under `benchmarks/src/bin/` (real wall clock, not
`start_paused`):

| Binary | Scenario | Env budget |
|--------|----------|------------|
| `soak_queue` | Job enqueue + follower drain | `SOAK_QUEUE_SECS` (default 15) |
| `soak_actor_store` | `RedbActorStateStore` + full cluster restart | `SOAK_ACTOR_STORE_SECS` |
| `soak_saga` | Partial saga + restart + `resume_saga` | `SOAK_SAGA_SECS` |
| `soak_session` | `ActorSession` + worker node restart | `SOAK_SESSION_SECS` |

Also: `soak` (sim), `soak_multi_raft`. Nightly `bench` job runs 60–120s budgets.
See [testing-coverage.md](../testing-coverage.md#ci-lane-mapping).

## Consequences

**Positive**

- Timing/partition bugs found **fast and reproducibly** (seeded sim), not via flaky containers
- Pure-FSM + Transport-trait design pays off directly in testability
- Clear per-crate ownership and CI lanes; PRs stay fast

**Negative**

- `trembita-sim` is a real engineering investment (virtual clock, scheduler, fault injection)
- Linearizability checker adds complexity
- Two CI lanes (fast/nightly) to maintain; E2E chaos infra (docker-compose, pumba) to keep green

## Related

- [testing-coverage.md](../testing-coverage.md) — living coverage matrix and gap tracker
- [status.md](../status.md) · [testing-coverage.md](../testing-coverage.md)
- [wire-protocol.md](wire-protocol.md) · [actor-state-redis.md](actor-state-redis.md) · [library-and-publishing.md](library-and-publishing.md) · [future-work-and-risks.md](future-work-and-risks.md)
