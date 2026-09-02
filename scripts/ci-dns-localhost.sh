#!/usr/bin/env bash
# RFC 6761 *.localhost — not wired on every CI resolver (GitLab saas-linux runners).
# Pin ordinal hostnames used by trembita::discovery integration tests.
set -euo pipefail

MARKER="# trembita discovery test hosts"
if grep -qF "$MARKER" /etc/hosts 2>/dev/null; then
  exit 0
fi

echo "127.0.0.1 trembita-0.localhost trembita-1.localhost $MARKER" >> /etc/hosts
