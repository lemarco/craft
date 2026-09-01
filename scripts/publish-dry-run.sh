#!/usr/bin/env bash
# Verify every publishable crate tarball (dependency order, same as publish-workspace.sh).
#
# Usage: ./scripts/publish-dry-run.sh

set -euo pipefail
cd "$(dirname "$0")/.."

PUBLISH_ORDER=(
    crafty-macros
    crafty-proto
    crafty-core
    crafty-storage
    crafty-net
    crafty-actor
    crafty-client
    crafty-dashboard
    crafty-http
    crafty-sim
    crafty-store-redis
    crafty
    crafty-node
)

for pkg in "${PUBLISH_ORDER[@]}"; do
    echo ">> publish dry-run: ${pkg}"
    cargo publish -p "$pkg" --dry-run --allow-dirty
done

echo "OK: publish dry-run (${#PUBLISH_ORDER[@]} crates)"
