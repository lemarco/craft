---
name: crafty-quality-gate
description: >-
  Run crafty local quality gates before commit or push — fmt, clippy, check,
  tests, doctests, release build; interpret failures. Use when verifying work
  is merge-ready, before MR, after fixes, or when CI/local gates fail.
---

# crafty quality gate

See [docs/process.md](../../docs/process.md) for tiers, CI, and autofix policy.

## Quick paths

| Goal | Command |
|------|---------|
| Pre-commit | `./scripts/gate.sh --tier commit --staged-only --stage` |
| Pre-push (full) | `./scripts/gate.sh --tier push` |
| CI parity only | `./scripts/ci-fast-lane.sh` |
| Release gate | `./scripts/release-gate.sh` or `./scripts/release.sh --dry-run` |
| Compile only | `./scripts/check-with-log.sh` |
| Single test | `./scripts/test-with-log.sh -p <crate> --test <name>` |
| Lock / hang | `./scripts/cargo-status.sh` → [cargo-diagnostics](../cargo-diagnostics/SKILL.md) |

Legacy wrappers: `quality-gate-pre-commit.sh`, `quality-gate-pre-push.sh`.

## Recommended workflow

1. **Preflight:** `./scripts/cargo-status.sh`
2. **Narrow:** `./scripts/test-with-log.sh -p <changed-crate> …`
3. **Static:** `./scripts/gate.sh --tier commit`
4. **Full:** `./scripts/gate.sh --tier push` before push/MR

**One cargo at a time.** Read `target/test-run.log` if the agent terminal is silent.

## Autofix

- Commit: staged crates only (lefthook re-stages into current commit)
- Push: full workspace; lefthook auto-commits fixable changes unless `CRAFTY_NO_AUTOFIX_COMMIT=1`
- Release: included in `chore(release): …` commit

## Failure triage

| Failure | Action |
|---------|--------|
| `cargo fmt` | autofix runs automatically; manual: `cargo fmt --all` |
| clippy `-D warnings` | Fix manually if not auto-fixable |
| compile error | Fix before test rerun |
| test fail | Re-run narrow test |
| doctest | `cargo test --doc -p <crate>` |
| doc link | `./scripts/check-doc-links.sh` |
| MSRV on release | `rustup toolchain install 1.90` |

## Merge-ready checklist

- [ ] `./scripts/gate.sh --tier commit` green
- [ ] Changed crates' tests green
- [ ] `docs/testing-coverage.md` updated if tests added/removed
- [ ] Conventional commit; `#issue` if applicable
- [ ] No secrets in diff

## CI parity

Fast lane = `ci-fast-lane.sh`. Push/release add examples, showcase, MSRV.
Release build: `gate.sh --tier push --release-build` or `lefthook run pre-push --tags release`.

Heavy (e2e, Redis `--ignored`): nightly or MR label **`run-heavy`**.
