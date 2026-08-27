---
name: craft-add-feature
description: >-
  Add a feature to craft following crate boundaries, trait ports, facade API,
  and ADR 029 testing. Use when implementing new functionality, extending
  consensus/actors/transport/storage, or adding a public API to the craft facade.
---

# craft add feature

## 1. Place the code

Use [`docs/architecture.md`](../../docs/architecture.md) dependency graph:

```
craft-node → craft (facade) → craft-actor → { craft-core, craft-net, craft-storage }
```

| Change type | Crate |
|-------------|-------|
| Raft semantics | `craft-core` (pure, no I/O) |
| Wire types | `craft-proto` |
| Storage backend | `craft-storage` (implement existing traits) |
| Transport / routes | `craft-net` |
| Runtime / actors | `craft-actor` |
| Client API | `craft-client` |
| Public API | `craft` facade re-exports |
| Sim / faults | `craft-sim` |

Do not put I/O in `craft-core`. Do not expose internal crates in public API unless intentional.

## 2. Need a new trait?

Apply ADR 030 litmus test: **≥2 implementations** (prod + test/sim). Otherwise use concrete types until a second adapter exists.

## 3. Tests (required)

Use [craft-testing](../craft-testing/SKILL.md):

- Core logic → unit or `craft-core/tests/`
- Persistence → driver + `run_contract` if storage changes
- Timing/partition → `craft-sim` with seed
- Runtime/API → integration over `LocalNetwork`

## 4. Docs

| Change | Update |
|--------|--------|
| New behavior | `docs/testing-coverage.md` |
| Design choice | ADR in `docs/decisions/` or note in existing ADR |
| Backlog item | `docs/backlog.md` status |
| Public API | rustdoc on facade types; CHANGELOG for user-visible changes |

## 5. Verify

```bash
./scripts/test-with-log.sh -p <crate> …
./scripts/quality-gate-pre-commit.sh
```

## Facade checklist

- [ ] Builder option on `CraftCluster::builder()` if user-facing
- [ ] Re-export types users need from `craft` crate root
- [ ] Example in `crates/craft/examples/` for non-trivial flows
