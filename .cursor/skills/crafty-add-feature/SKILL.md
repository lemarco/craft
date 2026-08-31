---
name: crafty-add-feature
description: >-
  Add a feature to crafty following crate boundaries, trait ports, facade API,
  and testing-strategy testing. Use when implementing new functionality, extending
  consensus/actors/transport/storage, or adding a public API to the crafty facade.
---

# crafty add feature

## 1. Place the code

Use [`docs/architecture.md`](../../docs/architecture.md) dependency graph:

```
crafty-node → crafty (facade) → crafty-actor → { crafty-core, crafty-net, crafty-storage }
```

| Change type | Crate |
|-------------|-------|
| Raft semantics | `crafty-core` (pure, no I/O) |
| Wire types | `crafty-proto` |
| Storage backend | `crafty-storage` (implement existing traits) |
| Transport / routes | `crafty-net` |
| Runtime / actors | `crafty-actor` |
| Client API | `crafty-client` |
| Public API | `crafty` facade re-exports |
| Sim / faults | `crafty-sim` |

Do not put I/O in `crafty-core`. Do not expose internal crates in public API unless intentional.

## 2. Need a new trait?

Apply architecture-style litmus test: **≥2 implementations** (prod + test/sim). Otherwise use concrete types until a second adapter exists.

## 3. Tests (required)

Use [crafty-testing](../crafty-testing/SKILL.md):

- Core logic → unit or `crafty-core/tests/`
- Persistence → driver + `run_contract` if storage changes
- Timing/partition → `crafty-sim` with seed
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

- [ ] Builder option on `CraftyCluster::builder()` if user-facing
- [ ] Re-export types users need from `crafty` crate root
- [ ] Example in [`examples/`](../../examples/README.md) or extend an existing showcase for non-trivial product flows
