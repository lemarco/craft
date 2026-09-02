#!/usr/bin/env bash
# trembita-init.sh — scaffold a new trembita product app (B-06).
#
# Usage: ./scripts/trembita-init.sh my-service
#
# Creates:
#   my-service/Cargo.toml
#   my-service/src/main.rs
#   my-service/docker-compose.yml
#   my-service/README.md

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMPLATE="${ROOT}/templates/trembita-app"
NAME="${1:-}"

die() { echo "error: $*" >&2; exit 1; }

[ -n "$NAME" ] || die "usage: $0 <project-name>"
echo "$NAME" | grep -Eq '^[a-z][a-z0-9_-]*$' \
    || die "project name must be lowercase alphanumeric (got: $NAME)"
[ -d "$TEMPLATE" ] || die "missing template dir: $TEMPLATE"
[ ! -e "$NAME" ] || die "target $NAME already exists"

render() {
    local src=$1 dst=$2
    sed "s/{{PROJECT_NAME}}/$NAME/g" "$src" > "$dst"
}

mkdir -p "$NAME/src"
render "$TEMPLATE/Cargo.toml.tpl" "$NAME/Cargo.toml"
render "$TEMPLATE/src/main.rs.tpl" "$NAME/src/main.rs"
render "$TEMPLATE/docker-compose.yml.tpl" "$NAME/docker-compose.yml"
render "$TEMPLATE/README.md.tpl" "$NAME/README.md"

echo "Created $NAME/"
echo "  cd $NAME && cargo run"
echo "  docker compose up   # 3-node local cluster (dev certs — see README)"
echo "Scenarios: ${ROOT}/docs/scenarios/README.md"
