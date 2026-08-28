#!/usr/bin/env bash
# Pre-commit quality gate — run manually or via lefthook pre-commit.
# Prefer: lefthook run pre-commit
#
# Doc + full test/release checks run on pre-push (see quality-gate-pre-push.sh).

set -euo pipefail
cd "$(dirname "$0")/.."

bash scripts/gate-fmt.sh --check
bash scripts/check-shell-scripts.sh
bash scripts/gate-clippy.sh
echo ">> pre-commit gate ok"
