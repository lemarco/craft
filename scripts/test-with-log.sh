#!/usr/bin/env bash
# Run tests with timestamped progress written to target/test-run.log (and stdout).
#
# Usage:
#   ./scripts/test-with-log.sh                          # full workspace
#   ./scripts/test-with-log.sh -p trembita-runtime group_rebalance
#   TREMBITA_LOG_REBALANCE=1 ./scripts/test-with-log.sh -p trembita
#
# Local iteration (faster — default-members, no check phase):
#   ./scripts/test-fast.sh -p trembita-runtime
#
# Env:
#   CARGO_LOG         — cargo internals (default: cargo::core=info for this script)
#   TREMBITA_SKIP_CHECK  — skip phase-1 cargo check (default: 1 when cargo-nextest exists)
#   TREMBITA_FORCE_CHECK — always run phase-1 cargo check
#   NEXTEST_PROFILE   — nextest profile (default: default; use ci for pre-push)
#   TREMBITA_LOG_REBALANCE — enable `trembita::rebalance=debug` tracing (call
#     `trembita_test_support::test_setup()` in tests, or use `trembita-node` binary)

set -euo pipefail
cd "$(dirname "$0")/.."

LOG="${TREMBITA_TEST_LOG:-target/test-run.log}"
mkdir -p target

log() {
  # ISO timestamp + message to log file and stderr (stderr shows in terminal immediately)
  printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "$LOG" >&2
}

log "=== test run start (args: $*) ==="
./scripts/cargo-status.sh 2>&1 | tee -a "$LOG" >&2

if [[ -f target/.cargo-lock ]]; then
  log "WARN: target/.cargo-lock exists — another cargo may be holding the build dir"
  log "      waiting up to 30s… (or kill the other cargo / rm the lock)"
  for _ in $(seq 1 30); do
    [[ ! -f target/.cargo-lock ]] && break
    sleep 1
  done
  if [[ -f target/.cargo-lock ]]; then
    log "ERROR: lock still present after 30s — aborting (run ./scripts/cargo-status.sh)"
    exit 1
  fi
fi

export CARGO_LOG="${CARGO_LOG:-cargo::core=info}"
if [[ $# -eq 0 ]]; then
  CARGO_ARGS=(--workspace --all-features)
else
  CARGO_ARGS=("$@")
fi

USE_NEXTEST=0
if command -v cargo-nextest >/dev/null 2>&1; then
  USE_NEXTEST=1
fi

# Phase 1: compile gate — skip when nextest is available (saves a full recompile pass).
RUN_CHECK=1
if [[ -n "${TREMBITA_SKIP_CHECK:-}" ]]; then
  RUN_CHECK=0
elif [[ "$USE_NEXTEST" -eq 1 && -z "${TREMBITA_FORCE_CHECK:-}" ]]; then
  RUN_CHECK=0
fi
if [[ -n "${TREMBITA_FORCE_CHECK:-}" ]]; then
  RUN_CHECK=1
fi

if [[ "$RUN_CHECK" -eq 1 ]]; then
  log "=== phase 1: cargo check ==="
  set +e
  cargo check "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
  check_status=${PIPESTATUS[0]}
  set -e
  if [[ "$check_status" -ne 0 ]]; then
    log "=== check failed exit=$check_status — skipping tests ==="
    exit "$check_status"
  fi
  log "=== phase 1 ok ==="
else
  log "=== phase 1: skipped (nextest compiles in one pass; TREMBITA_FORCE_CHECK=1 to enable check) ==="
fi

NEXTEST_PROFILE="${NEXTEST_PROFILE:-default}"

if [[ "$USE_NEXTEST" -eq 1 ]]; then
  log "=== phase 2: cargo nextest run (profile=$NEXTEST_PROFILE) ==="
  log "CARGO_LOG=$CARGO_LOG"
  log "running: cargo nextest run --profile $NEXTEST_PROFILE ${CARGO_ARGS[*]}"
  set +e
  cargo nextest run --profile "$NEXTEST_PROFILE" "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
  status=${PIPESTATUS[0]}
  set -e
else
  log "=== phase 2: cargo test ==="
  log "WARN: install cargo-nextest for parallel runs: ./scripts/install-dev-tools.sh"
  log "CARGO_LOG=$CARGO_LOG"
  log "running: cargo test ${CARGO_ARGS[*]}"
  set +e
  cargo test "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
  status=${PIPESTATUS[0]}
  set -e
fi

log "=== test run finished exit=$status ==="
exit "$status"
