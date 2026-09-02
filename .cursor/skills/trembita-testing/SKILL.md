---
name: trembita-testing
description: >-
  Add or extend tests in the trembita workspace following testing-strategy — choose unit,
  driver, sim, integration, or E2E layer; use LocalNetwork, run_contract,
  seeded trembita-sim; update docs/testing-coverage.md. Use when writing tests,
  fixing test gaps, adding regressions, or asking how to test trembita code.
---

# trembita testing

## 1. Choose layer

Read [`docs/testing-coverage.md`](../../docs/testing-coverage.md) for gaps.

| Symptom | Layer | Location |
|---------|-------|----------|
| Pure FSM / log / RNG | Unit | `trembita-core/src/` or `tests/` |
| Persist / recover / `take_persist` | Driver | `trembita-actor/tests/driver.rs` |
| Partition, drop, safety invariant | Sim | `trembita-sim/tests/` + seed |
| Runtime, actors, client wire | Integration | `crates/*/tests/` + `LocalNetwork` |
| Real QUIC + mTLS + processes | E2E | `e2e/` only |

## 2. Patterns

### Pure Raft (unit / integration)

Drive `RaftNode` with explicit `tick()` and injected messages — no async, no I/O.

### Storage contract

Add behavior to `run_contract<S>()` in `trembita-storage/tests/storage.rs`; run against Memory **and** Redb.

### Async cluster (integration)

```rust
let net = LocalNetwork::new();
let transport: Arc<dyn Transport> = Arc::new(net.clone());
// spawn_node + net.attach(id, NodeService)
```

Reuse KV fixtures from [`trembita-test-support`](../crates/trembita-test-support/src/kv.rs).

### Sim regression

```rust
let mut c = Cluster::new(3, seed);
c.set_fault(Fault { drop_percent, max_latency });
// ... schedule isolate/heal ...
c.run(steps);
// invariants checked every step in harness
```

Print `seed` in assertion messages for replay.

## 3. Checklist

- [ ] Lowest layer that reproduces the bug
- [ ] No `sleep` in sim; `start_paused` for client timeouts when possible
- [ ] Narrow test passes: `./scripts/test-with-log.sh -p <crate> --test <name>`
- [ ] Updated `docs/testing-coverage.md` (counts / gaps)

## References

- [testing-strategy](../../docs/decisions/testing-strategy.md)
- [testing-coverage.md](../../docs/testing-coverage.md)
- [cargo-diagnostics](../cargo-diagnostics/SKILL.md) — how to run tests safely
