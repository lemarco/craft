#!/usr/bin/env bash
# Run tests with timestamped progress written to target/test-run.log (and stdout).
#
# Usage:
#   ./scripts/test-with-log.sh                          # full workspace
#   ./scripts/test-with-log.sh -p craft-actor group_rebalance
#   CRAFT_LOG_REBALANCE=1 ./scripts/test-with-log.sh -p craft
#
# Env:
#   CARGO_LOG         — cargo internals (default: cargo::core=info for this script)
#   CRAFT_LOG_REBALANCE — enable `craft::rebalance=debug` tracing (call
#     `craft_test_support::test_setup()` in tests, or use `craft-node` binary)

set -euo pipefail
cd "$(dirname "$0")/.."

LOG="${CRAFT_TEST_LOG:-target/test-run.log}"
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

# Phase 1: compile gate — fail fast before linking/running tests.
if [[ -z "${CRAFT_SKIP_CHECK:-}" ]]; then
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
fi

log "=== phase 2: cargo test ==="
log "CARGO_LOG=$CARGO_LOG"
log "running: cargo test ${CARGO_ARGS[*]}"

# Unbuffered stderr from cargo via script/tee; stdout+stderr both logged
set +e
cargo test "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
status=${PIPESTATUS[0]}
set -e

log "=== test run finished exit=$status ==="
exit "$status"
