# Agent guide (trembita)

Distributed Raft runtime in Rust (product: **capabilities** on `TrembitaApp`). Read before making changes.

**Human contributors:** [CONTRIBUTING.md](CONTRIBUTING.md)

## Start here

1. [docs/status.md](docs/status.md) — current capabilities and limits
2. [docs/env.md](docs/env.md) — product `TREMBITA_*` surface (listen, data_dir, cert_dir, join)
3. [docs/decisions/framework-conventions.md](docs/decisions/framework-conventions.md) — scaffold layout (`manifest.rs` registry + `app.rs` gateway)
4. [docs/scenarios/README.md](docs/scenarios/README.md) — product scenarios (jobs, topics, workers, sessions, workflows)
5. [examples/README.md](examples/README.md) — product showcases (local + QUIC cluster)
6. [docs/backlog.md](docs/backlog.md) — open backlog · [docs/status.md](docs/status.md) — shipped index
7. [docs/architecture.md](docs/architecture.md) — crate graph
8. [docs/decisions/facade-layering.md](docs/decisions/facade-layering.md) — `trembita` vs `trembita-assembly` vs `trembita-showcase`
9. [docs/decisions/architecture-style.md](docs/decisions/architecture-style.md) — pure core, trait ports
10. [docs/decisions/product-scenarios.md](docs/decisions/product-scenarios.md) — capability-first platform, no mandatory Redis
11. [docs/decisions/product-terminology.md](docs/decisions/product-terminology.md) — product docs: capabilities vs runtime actors
12. [docs/decisions/gateway-session-naming.md](docs/decisions/gateway-session-naming.md) — `open_worker_session_*` vs legacy actor names
13. [docs/decisions/testing-strategy.md](docs/decisions/testing-strategy.md) — test pyramid
14. [docs/testing-coverage.md](docs/testing-coverage.md) — test inventory
15. [docs/process.md](docs/process.md) — gates, CI, release flow

## Cursor config

| Path | Purpose |
|------|---------|
| `.cursor/rules/trembita-architecture.mdc` | No I/O in core; ports & adapters |
| `.cursor/rules/trembita-deployment.mdc` | VPS/library-first — no K8s/Helm/operators in scope |
| `.cursor/rules/trembita-testing.mdc` | Test layer choice; update coverage doc |
| `.cursor/rules/trembita-quality-gate.mdc` | Pre-commit/push gates |
| `.cursor/rules/trembita-commits.mdc` | Small, focused, testable commits |
| `.cursor/rules/cargo-shell-safety.mdc` | One cargo, logging wrappers |
| `.cursor/skills/trembita-testing/` | How to write tests |
| `.cursor/skills/trembita-quality-gate/` | Merge-ready verification |
| `.cursor/skills/trembita-add-feature/` | Feature placement workflow |
| `.cursor/rules/trembita-publishing.mdc` | crates.io release; rate limits |
| `.cursor/skills/trembita-publishing/` | Tag + publish + resume workflow |
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

MSRV **1.94**. Conventional commits. GitLab issues as `#<number>`.
