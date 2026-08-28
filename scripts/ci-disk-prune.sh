#!/usr/bin/env bash
# Reclaim GitLab runner disk between heavy cargo steps (fast lane).
#
# saas-linux-small runners have ~25G root; unpacked debuginfo + cached target/
# can exhaust disk during the test link phase. Safe to drop incremental state and
# example binaries after clippy has checked them.

set -euo pipefail

root="${CI_PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$root"

if [[ -d target/debug ]]; then
  rm -rf target/debug/incremental
  rm -rf target/debug/examples
fi
rm -rf target/doc 2>/dev/null || true

if [[ -n "${CI:-}" ]]; then
  echo "disk after ci-disk-prune:"
  df -h "$root" | tail -1
  du -sh target .cargo 2>/dev/null || true
fi
