# Agent guide (crafty)

Distributed Raft + actor framework in Rust. Read before making changes.

**Human contributors:** [CONTRIBUTING.md](CONTRIBUTING.md)

## Start here

1. [docs/status.md](docs/status.md) — current capabilities and limits
2. [docs/scenarios/README.md](docs/scenarios/README.md) — product scenarios (jobs, topics, workers, sessions, workflows)
3. [examples/README.md](examples/README.md) — product showcases (local + QUIC cluster)
4. [docs/backlog.md](docs/backlog.md) — implementation backlog
5. [docs/architecture.md](docs/architecture.md) — crate graph
6. [docs/decisions/architecture-style.md](docs/decisions/architecture-style.md) — pure core, trait ports
7. [docs/decisions/product-scenarios.md](docs/decisions/product-scenarios.md) — actor-first platform, no mandatory Redis
8. [docs/decisions/testing-strategy.md](docs/decisions/testing-strategy.md) — test pyramid
9. [docs/testing-coverage.md](docs/testing-coverage.md) — test inventory
10. [docs/process.md](docs/process.md) — gates, CI, release flow

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
| `.cursor/rules/crafty-publishing.mdc` | crates.io release; rate limits |
| `.cursor/skills/crafty-publishing/` | Tag + publish + resume workflow |
| `.cursor/skills/cargo-diagnostics/` | Debug cargo lock / silent hangs |

## Quality (local)

```bash
lefthook install
./scripts/install-dev-tools.sh   # cargo-nextest (parallel tests)
./scripts/gate.sh --tier commit
./scripts/gate.sh --tier push
```

See [docs/process.md](docs/process.md) for gate tiers, CI lanes, and release.

Fast iteration while coding: `./scripts/test-fast.sh -p <crate>`.

MSRV **1.90**. Conventional commits. GitLab issues as `#<number>`.
