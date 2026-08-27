#!/usr/bin/env bash
# Fast local test path — default workspace members (skips craft-ops / e2e / redis),
# nextest when available, no redundant cargo check phase.
#
# Usage:
#   ./scripts/test-fast.sh                           # default-members, nextest profile=fast
#   ./scripts/test-heavy.sh                          # all tests + 250-case proptest
#   CRAFT_ALL_FEATURES=1 ./scripts/test-fast.sh      # enable json-wire etc.
#   ./scripts/test-fast.sh -p craft-actor group_rebalance
#   ./scripts/test-fast.sh --workspace               # full workspace (same as test-with-log)
#   CRAFT_FORCE_CHECK=1 ./scripts/test-fast.sh -p craft
#
# For CI parity / pre-push: ./scripts/test-with-log.sh --workspace --all-features

set -euo pipefail
cd "$(dirname "$0")/.."

LOG="${CRAFT_TEST_LOG:-target/test-run.log}"
mkdir -p target

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "$LOG" >&2
}

log "=== fast test start (args: $*) ==="
./scripts/cargo-status.sh 2>&1 | tee -a "$LOG" >&2

if [[ -f target/.cargo-lock ]]; then
  log "ERROR: target/.cargo-lock present — another cargo is running"
  exit 1
fi

export CARGO_LOG="${CARGO_LOG:-cargo::core=info}"

if [[ $# -eq 0 ]]; then
  # default-members only — omits craft-ops, craft-e2e-client, craft-store-redis,
  # craft-dashboard, craft-node
  CARGO_ARGS=()
  if [[ -n "${CRAFT_ALL_FEATURES:-}" ]]; then
    CARGO_ARGS+=(--all-features)
  fi
else
  CARGO_ARGS=("$@")
fi

# Optional compile gate (off by default — nextest/cargo test fail fast on compile errors).
if [[ -n "${CRAFT_FORCE_CHECK:-}" ]]; then
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

NEXTEST_PROFILE="${NEXTEST_PROFILE:-fast}"

if command -v cargo-nextest >/dev/null 2>&1; then
  log "=== cargo nextest run (profile=$NEXTEST_PROFILE) ==="
  set +e
  cargo nextest run --profile "$NEXTEST_PROFILE" "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
  status=${PIPESTATUS[0]}
  set -e
else
  log "WARN: cargo-nextest not installed — using cargo test (slower). Run: ./scripts/install-dev-tools.sh"
  log "=== cargo test ==="
  set +e
  cargo test "${CARGO_ARGS[@]}" 2>&1 | tee -a "$LOG"
  status=${PIPESTATUS[0]}
  set -e
fi

log "=== fast test finished exit=$status ==="
exit "$status"
