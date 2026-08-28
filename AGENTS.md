# Agent guide (crafty)

Distributed Raft + actor framework in Rust. Read before making changes.

## Start here

1. [docs/status.md](docs/status.md) — current capabilities and limits
2. [docs/architecture.md](docs/architecture.md) — crate graph
3. [docs/decisions/architecture-style.md](docs/decisions/architecture-style.md) — pure core, trait ports
4. [docs/decisions/testing-strategy.md](docs/decisions/testing-strategy.md) — test pyramid
5. [docs/testing-coverage.md](docs/testing-coverage.md) — test inventory

## Cursor config

| Path | Purpose |
|------|---------|
| `.cursor/rules/crafty-architecture.mdc` | No I/O in core; ports & adapters |
| `.cursor/rules/crafty-testing.mdc` | Test layer choice; update coverage doc |
| `.cursor/rules/crafty-quality-gate.mdc` | Pre-commit/push gates |
| `.cursor/rules/crafty-commits.mdc` | Small, focused, testable commits |
| `.cursor/rules/cargo-shell-safety.mdc` | One cargo, logging wrappers |
| `.cursor/skills/crafty-testing/` | How to write tests |
| `.cursor/skills/crafty-quality-gate/` | Merge-ready verification |
| `.cursor/skills/crafty-add-feature/` | Feature placement workflow |
| `.cursor/skills/cargo-diagnostics/` | Debug cargo lock / silent hangs |

## Quality (local)

```bash
lefthook install
./scripts/install-dev-tools.sh   # cargo-nextest (parallel tests)
./scripts/quality-gate-pre-commit.sh
./scripts/quality-gate-pre-push.sh
```

Fast iteration while coding: `./scripts/test-fast.sh -p <crate>`.

MSRV **1.90**. Conventional commits. GitLab issues as `#<number>`.
