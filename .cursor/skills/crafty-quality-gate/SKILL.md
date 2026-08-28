---
name: crafty-quality-gate
description: >-
  Run crafty local quality gates before commit or push — fmt, clippy, check,
  tests, doctests, release build; interpret failures. Use when verifying work
  is merge-ready, before MR, after fixes, or when CI/local gates fail.
---

# crafty quality gate

## Quick paths

| Goal | Command |
|------|---------|
| Pre-commit (static) | `./scripts/quality-gate-pre-commit.sh` |
| Pre-push (full) | `./scripts/quality-gate-pre-push.sh` |
| Compile only | `./scripts/check-with-log.sh` |
| Single test | `./scripts/test-with-log.sh -p <crate> --test <name>` |
| Lock / hang | `./scripts/cargo-status.sh` → see [cargo-diagnostics](../cargo-diagnostics/SKILL.md) |

## Recommended workflow

1. **Preflight:** `./scripts/cargo-status.sh` — no stray lock.
2. **Narrow:** `./scripts/test-with-log.sh -p <changed-crate> …`
3. **Static:** `./scripts/quality-gate-pre-commit.sh`
4. **Full:** `./scripts/quality-gate-pre-push.sh` before push/MR.

**One cargo at a time.** Read `target/test-run.log` if the agent terminal is silent.

## Failure triage

| Failure | Action |
|---------|--------|
| `cargo fmt` | `cargo fmt --all` |
| clippy `-D warnings` | Fix or allow with justification in PR |
| compile error | Fix before any test rerun |
| test fail | Re-run same narrow test; check seed for sim |
| doctest | `cargo test --doc -p <crate>` |
| example fail | `cargo check --examples -p <crate>` |

## Merge-ready checklist

- [ ] `./scripts/quality-gate-pre-commit.sh` green
- [ ] Changed crates' tests green (narrow → full workspace if touching many crates)
- [ ] `docs/testing-coverage.md` updated if tests added/removed
- [ ] Conventional commit message; `#issue` if applicable
- [ ] No secrets in diff

## CI parity

Fast lane matches pre-push + fmt/clippy/doc. Heavy (e2e, Redis `--ignored`) runs on schedule only — note in MR if change needs nightly.
