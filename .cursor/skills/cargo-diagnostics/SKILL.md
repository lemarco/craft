---
name: cargo-diagnostics
description: >-
  Run and debug Rust cargo builds/tests in this workspace without deadlocking
  on target/.cargo-lock or silent agent-shell hangs. Use when running cargo test,
  cargo build, quality gates, CI locally, tests appear stuck with no output,
  multiple terminals are queued, or debugging compile/test failures in craft.
---

# Cargo diagnostics (craft workspace)

## Problem this skill prevents

Parallel `cargo` invocations + Cursor agent shell queueing → exclusive
`target/.cargo-lock` → **zero stdout for minutes** → looks like an infinite hang.
This is infrastructure, not a Rust compile loop.

## Workflow (always follow)

### 1. Preflight

```bash
cd /path/to/craft   # workspace root (contains Cargo.toml workspace)
./scripts/cargo-status.sh
```

If lock present and cargo processes exist, wait or clean up **before** starting tests.

### 2. Compile gate, then tests

```bash
# Fastest local iteration (default-members, nextest, no duplicate check)
./scripts/test-fast.sh -p craft-core

# Full workspace (CI parity)
./scripts/test-with-log.sh --workspace --all-features

# Compile-only gate
./scripts/check-with-log.sh -p craft-actor
```

Install parallel test runner once: `./scripts/install-dev-tools.sh`

If `cargo check` fails, **do not** run test — fix compile errors first.
`test-with-log.sh` skips the check phase when `cargo-nextest` is installed
(set `CRAFT_FORCE_CHECK=1` to restore it). `test-fast.sh` never runs check
unless `CRAFT_FORCE_CHECK=1`.

### 3. Run tests (one command)

```bash
# Narrow (preferred while fixing)
./scripts/test-fast.sh -p craft-actor --test group_rebalance

# Full quality gate (only when ready; one invocation)
./scripts/test-with-log.sh --workspace --all-features
```

**Never** start a second `cargo` while the first is still running.

### 3. If the agent terminal shows no output

Read the log file directly:

```bash
tail -50 target/test-run.log
```

Look for:
- `=== test run finished exit=N ===`
- `Compiling …` / `error[E…]` / `test result:`

### 4. Recovery from a stuck queue

Run once in a **system** terminal (or after killing agent shells):

```bash
pkill -9 cargo rustc 2>/dev/null
rm -f target/.cargo-lock
./scripts/cargo-status.sh
```

Then a **single** `./scripts/test-with-log.sh …`.

## Scripts reference

| Script | Purpose |
|--------|---------|
| `scripts/cargo-status.sh` | Lock, cargo/rustc PIDs, tail of test log |
| `scripts/test-fast.sh` | Fast local tests (default-members, nextest, no check phase) |
| `scripts/test-with-log.sh` | Full workspace tests → `target/test-run.log` |
| `scripts/install-dev-tools.sh` | Install cargo-nextest (+ optional sccache) |

Env overrides:
- `CRAFT_TEST_LOG` — alternate log path (default `target/test-run.log`)
- `CARGO_LOG` — cargo verbosity (script default `cargo::core=info`)
- `CRAFT_FORCE_CHECK` — run cargo check before tests in fast/test-with-log scripts
- `CRAFT_SKIP_CHECK` — skip check phase in test-with-log.sh
- `NEXTEST_PROFILE` — nextest profile (`default` or `ci`)
- `CRAFT_LOG_REBALANCE=1` — multi-Raft rebalance debug lines

## Agent rules of thumb

1. **One cargo** per workspace per session.
2. **Log file first** when diagnosing silence — do not fire more cargo.
3. **Narrow tests first**, full workspace last.
4. Do not background cargo unless the user explicitly asks to run in background.
5. After fixing compile errors, re-run the **same** narrow test before full workspace.

## Success criteria

- `target/test-run.log` ends with `test run finished exit=0`
- `cargo-status.sh` shows no lock and no stray cargo processes
