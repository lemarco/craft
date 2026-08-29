#!/usr/bin/env bash
# RFC 6761 *.localhost — not wired on every CI resolver (GitLab saas-linux runners).
# Pin ordinal hostnames used by crafty::discovery integration tests.
set -euo pipefail

MARKER="# crafty discovery test hosts"
if grep -qF "$MARKER" /etc/hosts 2>/dev/null; then
  exit 0
fi

echo "127.0.0.1 crafty-0.localhost crafty-1.localhost $MARKER" >> /etc/hosts
