# Contributing to trembita

Thank you for helping improve trembita. This guide is for **human contributors**; AI agents should start from [AGENTS.md](AGENTS.md) instead.

## Before you code

1. [docs/status.md](docs/status.md) — what is shipped vs intentionally deferred
2. [docs/architecture.md](docs/architecture.md) — crate graph and dependency direction
3. [docs/decisions/architecture-style.md](docs/decisions/architecture-style.md) — pure core, ports & adapters
4. [docs/decisions/testing-strategy.md](docs/decisions/testing-strategy.md) — which test layer to use

**Product features:** [docs/scenarios/](docs/scenarios/README.md) and [examples/](examples/README.md).  
**Planned work:** [docs/backlog.md](docs/backlog.md) (open items in **Open work**).

## Local setup

```bash
lefthook install
./scripts/install-dev-tools.sh   # cargo-nextest (parallel tests)
```

MSRV is **1.90**. Edition **2024**. `unsafe` is forbidden workspace-wide.

## Quality gates

Full process diagram: [docs/process.md](docs/process.md).

| When | Command |
|------|---------|
| Pre-commit | `./scripts/gate.sh --tier commit` (or `lefthook run pre-commit`) |
| Pre-push / MR | `./scripts/gate.sh --tier push` |
| Release check | `./scripts/release.sh --dry-run` |

Hooks auto-fix fmt and fixable clippy lints (staged-only on commit; full workspace on push with an optional `chore: apply fmt/clippy autofix` commit). Disable push autofix commit with `TREMBITA_NO_AUTOFIX_COMMIT=1 git push`.

Fast iteration while coding:

```bash
./scripts/test-fast.sh -p <crate> --test <name>
./scripts/check-with-log.sh -p <crate>   # compile-only
```

See [.cursor/rules/cargo-shell-safety.mdc](.cursor/rules/cargo-shell-safety.mdc) — **one `cargo` at a time** in this workspace (lock queue looks like a hang).

## Where to put changes

| Change | Location |
|--------|----------|
| Pure Raft / FSM | `trembita-core` |
| Runtime, actors, queues | `trembita-actor`, facade in `trembita` |
| Public product API | `TrembitaApp` in `trembita`, docs in `docs/getting-started.md` |
| New port trait | core trait + prod adapter crate + test/sim adapter |
| Runnable product demo | `examples/<name>/` (standalone `Cargo.toml`) |
| Design rationale | new or updated ADR in `docs/decisions/` |
| Shipped capability | update [docs/status.md](docs/status.md) |
| Test inventory | update [docs/testing-coverage.md](docs/testing-coverage.md) |

Follow [docs/decisions/architecture-style.md](docs/decisions/architecture-style.md): adapters depend on core, never the reverse.

## Tests

Choose the layer from [docs/decisions/testing-strategy.md](docs/decisions/testing-strategy.md):

- Unit — pure logic in `#[cfg(test)]`
- Integration — `crates/*/tests/`
- Sim — `trembita-sim` for deterministic cluster scenarios
- E2E — `e2e/*.sh` for real QUIC/mTLS processes

New `#[ignore]` integration tests must note which CI heavy job runs them.

## Documentation

When shipping or changing user-visible behavior:

1. Update [docs/status.md](docs/status.md) (capabilities or limits)
2. Update the relevant scenario guide under [docs/scenarios/](docs/scenarios/README.md) if product-facing
3. Add or extend an ADR in [docs/decisions/](docs/decisions/) for non-obvious design choices
4. Doc-links are checked in the commit gate (`gate.sh --tier commit`)

Published crates require rustdoc (`missing_docs = "deny"`). Audit: `./scripts/docs-missing-audit.sh`.

## Commits and issues

- [Conventional commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`
- Reference GitLab issues as `#<number>` in commit messages and MR descriptions
- Keep commits small and focused; one logical change per commit

## Release (maintainers)

See [docs/releasing.md](docs/releasing.md) and [.cursor/skills/trembita-publishing/](.cursor/skills/trembita-publishing/SKILL.md).

## Related

- [AGENTS.md](AGENTS.md) — agent/AI entry point
- [docs/README.md](docs/README.md) — documentation hub
- [docs/testing-coverage.md](docs/testing-coverage.md) — test matrix
