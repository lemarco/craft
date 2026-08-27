# Agent guide (craft)

Distributed Raft + actor framework in Rust. Read before making changes.

## Start here

1. [docs/architecture.md](docs/architecture.md) — crate graph
2. [docs/decisions/architecture-style.md](docs/decisions/architecture-style.md) — pure core, trait ports
3. [docs/decisions/testing-strategy.md](docs/decisions/testing-strategy.md) — test pyramid
4. [docs/testing-coverage.md](docs/testing-coverage.md) — what is covered, known gaps

## Cursor config

| Path | Purpose |
|------|---------|
| `.cursor/rules/craft-architecture.mdc` | No I/O in core; ports & adapters |
| `.cursor/rules/craft-testing.mdc` | Test layer choice; update coverage doc |
| `.cursor/rules/craft-quality-gate.mdc` | Pre-commit/push gates |
| `.cursor/rules/craft-commits.mdc` | Small, focused, testable commits |
| `.cursor/rules/cargo-shell-safety.mdc` | One cargo, logging wrappers |
| `.cursor/skills/craft-testing/` | How to write tests |
| `.cursor/skills/craft-quality-gate/` | Merge-ready verification |
| `.cursor/skills/craft-add-feature/` | Feature placement workflow |
| `.cursor/skills/cargo-diagnostics/` | Debug cargo lock / silent hangs |

## Quality (local)

```bash
lefthook install
./scripts/quality-gate-pre-commit.sh
./scripts/quality-gate-pre-push.sh
```

MSRV **1.98**. Conventional commits. GitLab issues as `#<number>`.
