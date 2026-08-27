#!/usr/bin/env bash
# Guard against parallel cargo invocations (silent hangs on target/.cargo-lock).
#
# Usage:
#   ./scripts/cargo-wait-lock.sh              # fail immediately if locked
#   ./scripts/cargo-wait-lock.sh --wait 60    # wait up to 60s

set -euo pipefail
cd "$(dirname "$0")/.."

WAIT_SECS=0
if [[ "${1:-}" == "--wait" ]]; then
  WAIT_SECS="${2:-30}"
fi

if [[ ! -f target/.cargo-lock ]]; then
  exit 0
fi

if [[ "$WAIT_SECS" -eq 0 ]]; then
  echo "error: target/.cargo-lock is held — another cargo is running." >&2
  echo "       run ./scripts/cargo-status.sh or stop the other build." >&2
  exit 1
fi

echo "waiting up to ${WAIT_SECS}s for target/.cargo-lock…" >&2
for _ in $(seq 1 "$WAIT_SECS"); do
  [[ ! -f target/.cargo-lock ]] && exit 0
  sleep 1
done

echo "error: target/.cargo-lock still held after ${WAIT_SECS}s" >&2
exit 1
