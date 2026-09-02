#!/usr/bin/env bash
#
# gateway_jobs.sh — integration test: HTTP jobs batch through product gateway
# with Bearer auth (B-14c). Runs in-process via trembita test harness.
#
#   ./e2e/gateway_jobs.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "running gateway HTTP jobs integration test…"
./scripts/test-fast.sh -p trembita --test gateway_jobs_http -- --nocapture
echo "GATEWAY JOBS OK ✓"
