#!/usr/bin/env bash
# Pre-push quality gate — run manually or via lefthook pre-push.
# Aligned with ci-fast-lane.sh + examples, showcase, MSRV, optional release build.
#
# Env: CRAFTY_GATE_AUTOFIX, CRAFTY_AUTOFIX_COMMIT, CRAFTY_NO_AUTOFIX_COMMIT,
#      CRAFTY_SKIP_RELEASE, CRAFTY_HOOK_LOG

set -euo pipefail
exec bash scripts/gate.sh --tier push "$@"
