#!/usr/bin/env bash
# Pre-commit quality gate — run manually or via lefthook pre-commit.
# Prefer: lefthook run pre-commit (shellcheck on staged .sh only).
#
# Doc + full test/release checks run on pre-push (gate.sh --tier push).

set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/gate.sh --tier commit --staged-only --stage
bash scripts/check-shell-scripts.sh
