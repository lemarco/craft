#!/usr/bin/env bash
# B-50 — CI heavy lane (MR label run-heavy / nightly): local elastic smoke without
# the full e2e/elastic_lb.sh compose stack. Wraps scripts/local-cluster.sh (B-42):
# 4th joiner, nginx LB on :18290, /ready spread, cluster session, GET /e2e/whoami.
#
#   bash scripts/ci-local-elastic-smoke.sh
#
# Needs: Rust toolchain, curl, Docker (dev/local-3node nginx LB). Debug trembita-cli
# provides `dev cluster-*` (release builds the realtime showcase binary).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: $1 required" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd cargo
require_cmd docker

if ! docker info >/dev/null 2>&1; then
  echo "error: docker daemon not reachable (set DOCKER_HOST for DinD on CI)" >&2
  exit 1
fi

cleanup() {
  echo ">> ci-local-elastic-smoke: cleanup"
  "$ROOT/scripts/local-cluster.sh" stop || true
}
trap cleanup EXIT

export TREMBITA_LOCAL_CLUSTER_SHOWCASE="${TREMBITA_LOCAL_CLUSTER_SHOWCASE:-realtime}"
export GATEWAY_TOKEN="${GATEWAY_TOKEN:-dev-secret}"
export TREMBITA_GATEWAY_SESSION_SECRET="${TREMBITA_GATEWAY_SESSION_SECRET:-trembita-local-3node-dev-secret}"

echo ">> ci-local-elastic-smoke: setup (certs + release ${TREMBITA_LOCAL_CLUSTER_SHOWCASE})"
"$ROOT/scripts/local-cluster.sh" setup

echo ">> ci-local-elastic-smoke: elastic-up (4 nodes, staged join + /ready)"
"$ROOT/scripts/local-cluster.sh" elastic-up

echo ">> ci-local-elastic-smoke: lb-up (nginx round-robin)"
"$ROOT/scripts/local-cluster.sh" lb-up

echo ">> ci-local-elastic-smoke: elastic-smoke (/ready + session + cap)"
"$ROOT/scripts/local-cluster.sh" elastic-smoke

echo "PASS: ci-local-elastic-smoke (B-50)"
