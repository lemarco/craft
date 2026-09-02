---
name: trembita-add-feature
description: >-
  Add a feature to trembita following crate boundaries, trait ports, facade API,
  and testing-strategy testing. Use when implementing new functionality, extending
  consensus/actors/transport/storage, or adding a public API to the trembita facade.
---

# trembita add feature

## 1. Place the code

Use [`docs/architecture.md`](../../docs/architecture.md) dependency graph:

```
trembita-node → trembita (facade) → trembita-actor → { trembita-core, trembita-net, trembita-storage }
```

| Change type | Crate |
|-------------|-------|
| Raft semantics | `trembita-core` (pure, no I/O) |
| Wire types | `trembita-proto` |
| Storage backend | `trembita-storage` (implement existing traits) |
| Transport / routes | `trembita-net` |
| Runtime / actors | `trembita-actor` |
| Client API | `trembita-client` |
| Public API | `trembita` facade re-exports |
| Sim / faults | `trembita-sim` |

Do not put I/O in `trembita-core`. Do not expose internal crates in public API unless intentional.

## 2. Need a new trait?

Apply architecture-style litmus test: **≥2 implementations** (prod + test/sim). Otherwise use concrete types until a second adapter exists.

## 3. Tests (required)

Use [trembita-testing](../trembita-testing/SKILL.md):

- Core logic → unit or `trembita-core/tests/`
- Persistence → driver + `run_contract` if storage changes
- Timing/partition → `trembita-sim` with seed
- Runtime/API → integration over `LocalNetwork`

## 4. Docs

| Change | Update |
|--------|--------|
| New behavior | `docs/testing-coverage.md` |
| Design choice | ADR in `docs/decisions/` or note in existing ADR |
| Backlog item | `docs/status.md` |
| Public API | rustdoc on facade types; CHANGELOG for user-visible changes |

## 5. Verify

```bash
./scripts/test-with-log.sh -p <crate> …
./scripts/quality-gate-pre-commit.sh
```

## Facade checklist

- [ ] Builder option on `TrembitaCluster::builder()` if user-facing
- [ ] Re-export types users need from `trembita` crate root
- [ ] Example in [`examples/`](../../examples/README.md) or extend an existing showcase for non-trivial product flows
