#!/usr/bin/env bash
#
# sync-website-content.sh — copy docs/ into website/ with link rewrites for VitePress.
#
# Generated paths are gitignored; run via `npm run sync` before dev/build.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS="$ROOT/docs"
WEB="$ROOT/website"
REPO="https://gitlab.com/lemarco/trembita"
REPO_BLOB="$REPO/-/blob/main"
REPO_TREE="$REPO/-/tree/main"

transform_md() {
  sed -E \
    -e "s|\(\.\./examples/([^)]+)\)|($REPO_TREE/examples/\1)|g" \
    -e "s|\(\.\./crates/([^)]+)\)|($REPO_BLOB/crates/\1)|g" \
    -e "s|\(\.\./CHANGELOG\.md\)|(/changelog)|g" \
    -e "s|\(\.\./CONTRIBUTING\.md\)|($REPO_BLOB/CONTRIBUTING.md)|g" \
    -e "s|\(\.\./AGENTS\.md\)|($REPO_BLOB/AGENTS.md)|g" \
    -e "s|\(\.\./README\.md\)|($REPO_BLOB/README.md)|g" \
    -e "s|\(\.\./getting-started\.md\)|(/guide/getting-started)|g" \
    -e "s|\(getting-started\.md\)|(/guide/getting-started)|g" \
    -e "s|\(\.\./status\.md\)|(/reference/status)|g" \
    -e "s|\(status\.md\)|(/reference/status)|g" \
    -e "s|\(\.\./architecture\.md\)|(/reference/architecture)|g" \
    -e "s|\(architecture\.md\)|(/reference/architecture)|g" \
    -e "s|\(\.\./protocol\.md\)|(/reference/protocol)|g" \
    -e "s|\(protocol\.md\)|(/reference/protocol)|g" \
    -e "s|\(\.\./certs\.md\)|(/reference/certs)|g" \
    -e "s|\(certs\.md\)|(/reference/certs)|g" \
    -e "s|\(\.\./backlog\.md\)|($REPO_BLOB/docs/backlog.md)|g" \
    -e "s|\(backlog\.md\)|($REPO_BLOB/docs/backlog.md)|g" \
    -e "s|\(\.\./testing-coverage\.md\)|($REPO_BLOB/docs/testing-coverage.md)|g" \
    -e "s|\(\.\./releasing\.md\)|($REPO_BLOB/docs/releasing.md)|g" \
    -e "s|\(\.\./process\.md\)|($REPO_BLOB/docs/process.md)|g" \
    -e "s|\(\.\./ops/([^)]+)\)|($REPO_BLOB/docs/ops/\1)|g" \
    -e "s|\(\.\./scenarios/README\.md\)|(/scenarios/)|g" \
    -e "s|\(\.\./scenarios/([^)#]+)(#[^)]*)?\)|(/scenarios/\1\2)|g" \
    -e "s|\(scenarios/([^)#]+)(#[^)]*)?\)|(/scenarios/\1\2)|g" \
    -e "s|\(\.\./decisions/([^)#]+)(#[^)]*)?\)|(/design/\1\2)|g" \
    -e "s|\(decisions/([^)#]+)(#[^)]*)?\)|(/design/\1\2)|g" \
    -e "s|\(docs/decisions/([^)#]+)(#[^)]*)?\)|(/design/\1\2)|g" \
    -e "s|\(\.\./examples/README\.md\)|(/examples)|g" \
    -e "s|\[library-and-publishing\]\(docs/decisions/library-and-publishing\.md\)|[library & publishing](/design/library-and-publishing)|g"
}

copy_transform() {
  local src="$1"
  local dst="$2"
  mkdir -p "$(dirname "$dst")"
  transform_md < "$src" > "$dst"
  echo "  $(realpath --relative-to="$ROOT" "$dst")"
}

echo ">> syncing website content from docs/"

rm -rf "$WEB/guide" "$WEB/scenarios" "$WEB/reference" "$WEB/design"
mkdir -p "$WEB/guide" "$WEB/scenarios" "$WEB/reference" "$WEB/design"

copy_transform "$DOCS/getting-started.md" "$WEB/guide/getting-started.md"
copy_transform "$DOCS/scenarios/README.md" "$WEB/scenarios/index.md"

for f in "$DOCS/scenarios"/*.md; do
  base="$(basename "$f")"
  [ "$base" = "README.md" ] && continue
  copy_transform "$f" "$WEB/scenarios/${base%.md}.md"
done

for f in status architecture protocol certs; do
  copy_transform "$DOCS/${f}.md" "$WEB/reference/${f}.md"
done

DESIGN_ADRS=(
  deployment-model
  product-scenarios
  job-queue
  event-topics
  gateway-identity
  multi-raft
  upgrade-coordinator
  wire-protocol
  security
  library-and-publishing
  workload-governor
  cluster-elasticity
  cluster-membership
  actor-state-store
  cross-node-actors
  introspect-api
  framework-conventions
)

for adr in "${DESIGN_ADRS[@]}"; do
  src="$DOCS/decisions/${adr}.md"
  if [ -f "$src" ]; then
    copy_transform "$src" "$WEB/design/${adr}.md"
  fi
done

# Changelog: strip .md extension from in-page anchors and fix doc links.
transform_md < "$ROOT/CHANGELOG.md" \
  | sed -E \
      -e 's|\(#([0-9]+\.[0-9]+\.[0-9]+[^)]*)\)|(#\1)|g' \
      -e 's|\[library-and-publishing\]\(/design/library-and-publishing\)|library-and-publishing|g' \
  > "$WEB/changelog.md"
echo "  website/changelog.md"

echo "OK: website content synced."
