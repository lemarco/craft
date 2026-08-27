# Architecture style — pragmatic ports & adapters

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Is craft built with **hexagonal architecture** (ports & adapters)? The concern: consensus + actor systems are notoriously hard to test if consensus logic is tangled with sockets, disk, and clocks. We already leaned this way implicitly:

- `craft-core` is a **pure FSM** ([backlog](../backlog.md) A1) — no I/O.
- Persistence, transport, and external state are **traits** ([wire-transport](wire-transport.md), Track B, [actor-state-redis](actor-state-redis.md)).
- Deterministic simulation ([testing-strategy](testing-strategy.md)) depends on swapping real adapters for in-memory/virtual ones.

The open question is how *strictly* to apply the pattern.

## Decision

**Adopt ports-and-adapters as a principle at crate/trait boundaries; avoid layered ceremony inside crates.**

### The core is pure (hard rule)

`craft-core` (and `craft-proto`) contain **no I/O**: no `tokio`, no sockets, no disk, no wall-clock. Consensus is expressed as `RaftInput → (state, RaftOutput)`. Effects are returned as data for an outer runtime to execute. Enforced by keeping `tokio`/`quinn`/`redb` out of those crates' dependency trees.

### Ports are traits, adapters are crates

| Port (trait) | Prod adapter | Test adapter |
|--------------|--------------|--------------|
| `LogStore` / `HardStateStore` / `SnapshotStore` | `redb` (`craft-storage`) | in-memory |
| `Transport` | `quinn`/`h3` (`craft-net`) | sim transport (`craft-sim`) |
| `ActorStateStore` | Redis (`craft-store-redis`) | in-memory / fake |
| `Clock` | system time (runtime) | virtual clock (`craft-sim`) |

**Litmus test:** every port must have **≥ 2 implementations** (production + test/sim). A trait with one impl is probably not a real boundary and should not be abstracted yet.

### Time is a dependency

A `Clock` port is injected wherever timing matters (election/heartbeat timers). The domain never calls `Instant::now()` or `tokio::time` directly — required for deterministic, seed-reproducible simulation ([testing-strategy](testing-strategy.md)).

### Dependency direction

Adapters depend on the core, never the reverse:

```
craft-node → craft → craft-actor → { craft-core, craft-net, craft-storage }
                          │                  ▲  (adapters implement traits the
                          └── craft-client ──┘   inner crates/core define)
```

`craft-core` depends only on `craft-proto`. Runtime crates wire adapters into the core.

### Deliberate non-goals (the "ceremony" we skip)

- **No** `domain/application/infrastructure` sub-layering inside each crate.
- **No** DTO mapping between internal layers; types flow directly.
- **`craft-proto` is a shared kernel**, not a leak: wire types are the *contract* that crosses boundaries by design. The rule is about **no I/O in core logic**, not "no shared types."
- **No** abstracting a port until a second implementation actually exists.

## Consequences

**Positive**

- The one rule that matters (pure, I/O-free core) is explicit and CI-enforceable (dependency check)
- Deterministic simulation, fuzzing, and property tests fall out naturally
- Parallel tracks (A/B/C/D) integrate through stable trait ports
- Idiomatic Rust — traits + crates *are* the hexagon; no extra scaffolding

**Negative**

- Effects-as-data in `craft-core` is less obvious than direct calls (documented pattern)
- Discipline required: reviewers must reject I/O creeping into core crates
- Some indirection through traits on hot paths (acceptable; monomorphized)

## Related

- [testing-strategy.md](testing-strategy.md) — why the pure core matters
- [wire-transport.md](wire-transport.md) · [actor-state-redis.md](actor-state-redis.md)
- [architecture.md](../architecture.md) · [backlog.md](../backlog.md)
