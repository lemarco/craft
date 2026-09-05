#!/usr/bin/env bash
# Soak gateway dispatch tests (HTTP + auth + rate limit) — run before release.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUNS="${1:-20}"
echo "soak-gateway: $RUNS iterations"
for i in $(seq 1 "$RUNS"); do
  echo "[$i/$RUNS] gateway_auth_dispatch"
  cargo test -p trembita --features http-jobs --test gateway_auth_dispatch -- --test-threads=1
done
echo "soak-gateway: ok"
