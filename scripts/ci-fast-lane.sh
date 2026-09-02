#!/usr/bin/env bash
# CI fast lane — fmt, clippy, tests, doctests, doc, shellcheck, doc-links,
# publish dry-run. Shared by GitLab CI (MR/branch/tag) and local pre-push base.
#
# Usage: ./scripts/ci-fast-lane.sh
#
# Env:
#   CRAFTY_CI_DISK_PRUNE=1  — run ci-disk-prune.sh between heavy steps (CI runners)

set -euo pipefail
cd "$(dirname "$0")/.."

STEPS=(fmt clippy tests doctests doc shellcheck doc-links publish-dry-run)
for step in "${STEPS[@]}"; do
  bash scripts/gate-step.sh "$step"
done

printf '[%s] >> ci fast lane ok\n' "$(date -Is)" >&2
