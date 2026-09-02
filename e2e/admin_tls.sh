#!/usr/bin/env bash
#
# admin_tls.sh — smoke-test admin HTTPS on node1 (production ops follow-up).
#
# Requires the docker-compose cluster from linearizability.sh / run.sh.
# node1 publishes TLS admin on host port 18443.

set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=lib.sh
. ./lib.sh

ensure_ca_file
echo "GET https://$HOST:18443/ready (CA $(basename "$CA_FILE"))"
body=$(curl -sf -m 5 --cacert "$CA_FILE" "https://$HOST:18443/ready")
echo "$body" | grep -q '"member":true' || {
    echo "FAIL: /ready body missing member:true: $body"
    exit 1
}
echo "ADMIN TLS OK ✓"
