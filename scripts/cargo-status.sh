#!/usr/bin/env bash
# Quick snapshot: why is cargo stuck / silent?
# Usage: ./scripts/cargo-status.sh

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT="$(pwd)"
LOCK="$ROOT/target/.cargo-lock"
LOG="$ROOT/target/test-run.log"

echo "=== craft cargo status @ $(date -Is) ==="
echo "root: $ROOT"
echo

if [[ -f "$LOCK" ]]; then
  echo "LOCK: present ($LOCK)"
  echo "  holder pid(s) from lsof (if available):"
  if command -v lsof >/dev/null 2>&1; then
    lsof "$LOCK" 2>/dev/null || echo "  (lsof: none or permission denied)"
  else
    echo "  (install lsof to see lock holder)"
  fi
else
  echo "LOCK: absent (no cargo build in progress, or stale lock already removed)"
fi
echo

echo "cargo processes:"
if pgrep -a cargo >/dev/null 2>&1; then
  pgrep -a cargo
else
  echo "  (none)"
fi
echo

echo "rustc processes:"
if pgrep -a rustc >/dev/null 2>&1; then
  pgrep -a rustc | head -20
  count=$(pgrep -c rustc || true)
  [[ "$count" -gt 20 ]] && echo "  … and $((count - 20)) more"
else
  echo "  (none)"
fi
echo

if [[ -f "$LOG" ]]; then
  echo "last 15 lines of $LOG:"
  tail -15 "$LOG" | sed 's/^/  /'
else
  echo "no test log yet ($LOG) — run ./scripts/test-with-log.sh"
fi
echo
echo "tip: parallel 'cargo test' from the agent queues on target/.cargo-lock with zero stdout until the lock is free."
