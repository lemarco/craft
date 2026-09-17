#!/usr/bin/env bash
# B-49 — manual two-version lab (examples/self-update). Not CI-fast.
#
#   ./scripts/rolling-upgrade-lab.sh setup
#   ./scripts/rolling-upgrade-lab.sh up
#   ./scripts/rolling-upgrade-lab.sh trigger
#   ./scripts/rolling-upgrade-lab.sh stop
#
# Automated regression (fast CI): ./scripts/test-fast.sh -p trembita --test rolling_upgrade_proof b49_
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LAB="$ROOT/examples/self-update"
exec "$LAB/cluster.sh" "$@"
