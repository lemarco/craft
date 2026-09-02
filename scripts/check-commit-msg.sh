#!/usr/bin/env bash
# Conventional commit message gate (GitLab workflow / library-and-publishing).
#
# Usage: scripts/check-commit-msg.sh .git/COMMIT_EDITMSG

set -euo pipefail

msg_file="${1:?commit message file required}"
subject=$(head -n1 "$msg_file")

# Allow merges, reverts, releases, and Dependabot-style subjects.
if [[ "$subject" =~ ^Merge ]] || [[ "$subject" =~ ^Revert ]] || [[ "$subject" =~ ^chore\(release\): ]]; then
  exit 0
fi

if [[ "$subject" =~ ^(feat|fix|chore|docs|refactor|test|ci|build|perf)(\([a-z0-9_.-]+\))?!?:\ .+ ]]; then
  exit 0
fi

cat >&2 <<'EOF'
commit message must follow conventional commits, e.g.:

  feat: add group rebalance control plane
  fix(trembita-actor): correct NodeId comparison in test
  chore: update lefthook gates

types: feat | fix | chore | docs | refactor | test | ci | build | perf
EOF
exit 1
