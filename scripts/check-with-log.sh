#!/usr/bin/env bash
# Compile-only gate with timestamped log (fast fail before tests).
#
# Usage:
#   ./scripts/check-with-log.sh                    # full workspace
#   ./scripts/check-with-log.sh -p trembita-actor

set -euo pipefail
cd "$(dirname "$0")/.."

LOG="${TREMBITA_TEST_LOG:-target/test-run.log}"
mkdir -p target

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "$LOG" >&2
}

log "=== check start (args: $*) ==="
./scripts/cargo-status.sh 2>&1 | tee -a "$LOG" >&2

if [[ -f target/.cargo-lock ]]; then
  log "ERROR: target/.cargo-lock present — another cargo is running"
  exit 1
fi

if [[ $# -eq 0 ]]; then
  CARGO_ARGS=(--workspace --all-features)
else
  CARGO_ARGS=("$@")
fi

export CARGO_LOG="${CARGO_LOG:-cargo::core=info}"
log "running: cargo check ${CARGO_ARGS[*]}"

set +e
cargo check "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
status=${PIPESTATUS[0]}
set -e

log "=== check finished exit=$status ==="
exit "$status"
