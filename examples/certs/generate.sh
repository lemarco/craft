#!/usr/bin/env bash
#
# generate.sh — mint an mTLS PKI for a craft cluster (security, cert-provisioning).
#
# Produces a cluster CA, per-node certificates (whose name binds to a NodeId as
# `craft-node-<id>`, matching the SNI craft dials with), and client certificates
# for the RemoteClient. Uses only `openssl` so it works on Linux (OpenSSL) and
# macOS (LibreSSL) alike — no Rust toolchain required.
#
# Usage:
#   # First node — creates a fresh CA alongside the node cert:
#   ./generate.sh --node-id 1 --out ./certs
#
#   # Additional nodes — reuse the cluster CA:
#   ./generate.sh --node-id 2 --out ./certs --ca ./certs/ca.pem --ca-key ./certs/ca.key
#
#   # A client certificate for an app using RemoteClient:
#   ./generate.sh --client --name my-app --out ./certs --ca ./certs/ca.pem --ca-key ./certs/ca.key
#
#   # Just the CA:
#   ./generate.sh --ca-only --out ./certs
#
# See docs/certs.md for the manual OpenSSL equivalent, the env vars craft-node
# reads, and rotation guidance.

set -euo pipefail

NODE_ID=""
CLIENT_NAME=""
MODE=""            # node | client | ca-only
OUT="./certs"
CA_CERT=""
CA_KEY=""
NODE_DAYS=825      # ~27 months; keep under the 825d TLS server-cert norm
CA_DAYS=3650
CURVE="prime256v1" # NIST P-256 (ECDSA), supported by rustls' ring provider

die() { echo "error: $*" >&2; exit 1; }

usage() {
    sed -n '3,30p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --node-id)  MODE="node"; NODE_ID="${2:?--node-id needs a value}"; shift 2 ;;
        --client)   MODE="client"; shift ;;
        --name)     CLIENT_NAME="${2:?--name needs a value}"; shift 2 ;;
        --ca-only)  MODE="ca-only"; shift ;;
        --out)      OUT="${2:?--out needs a value}"; shift 2 ;;
        --ca)       CA_CERT="${2:?--ca needs a value}"; shift 2 ;;
        --ca-key)   CA_KEY="${2:?--ca-key needs a value}"; shift 2 ;;
        --node-days) NODE_DAYS="${2:?}"; shift 2 ;;
        --ca-days)  CA_DAYS="${2:?}"; shift 2 ;;
        -h|--help)  usage 0 ;;
        *)          die "unknown argument: $1 (try --help)" ;;
    esac
done

command -v openssl >/dev/null 2>&1 || die "openssl not found on PATH"
[ -n "$MODE" ] || die "specify one of --node-id N, --client --name NAME, or --ca-only"

mkdir -p "$OUT"

# Generate a P-256 private key at $1 (PKCS#8 PEM).
gen_key() {
    openssl genpkey -algorithm EC -pkeyopt "ec_paramgen_curve:$CURVE" -out "$1" 2>/dev/null
    chmod 600 "$1"
}

# Create the cluster CA at $OUT/ca.{pem,key} unless it already exists.
make_ca() {
    CA_CERT="$OUT/ca.pem"
    CA_KEY="$OUT/ca.key"
    if [ -f "$CA_CERT" ] && [ -f "$CA_KEY" ]; then
        echo "reusing existing CA at $CA_CERT"
        return
    fi
    gen_key "$CA_KEY"
    local cfg; cfg="$(mktemp)"
    cat > "$cfg" <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no
[dn]
CN = craft cluster CA
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
    openssl req -x509 -new -key "$CA_KEY" -days "$CA_DAYS" -out "$CA_CERT" -config "$cfg"
    rm -f "$cfg"
    echo "created cluster CA: $CA_CERT (guard $CA_KEY carefully)"
}

# Ensure we have a CA to sign with: use the provided one, else mint a new one.
ensure_ca() {
    if [ -n "$CA_CERT" ] || [ -n "$CA_KEY" ]; then
        [ -n "$CA_CERT" ] && [ -n "$CA_KEY" ] || die "pass both --ca and --ca-key (or neither to create one)"
        [ -f "$CA_CERT" ] || die "CA cert not found: $CA_CERT"
        [ -f "$CA_KEY" ]  || die "CA key not found: $CA_KEY"
    else
        make_ca
    fi
}

# Sign a CSR at $1 into cert $2 using extension file $3.
sign() {
    openssl x509 -req -in "$1" -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
        -days "$NODE_DAYS" -extfile "$3" -out "$2" 2>/dev/null
}

make_node() {
    local id="$1" key csr crt ext cn
    cn="craft-node-$id"
    key="$OUT/node-$id.key"; csr="$OUT/node-$id.csr"; crt="$OUT/node-$id.pem"; ext="$(mktemp)"
    gen_key "$key"
    openssl req -new -key "$key" -out "$csr" -subj "/CN=$cn" 2>/dev/null
    cat > "$ext" <<EOF
subjectAltName = DNS:$cn
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
EOF
    sign "$csr" "$crt" "$ext"
    rm -f "$csr" "$ext"
    echo "node $id cert: $crt (CN/SAN=$cn)"
    echo "  CRAFT_NODE_CERT=$crt CRAFT_NODE_KEY=$key CRAFT_CA_CERT=$CA_CERT"
}

make_client() {
    local name="$1" key csr crt ext cn
    [ -n "$name" ] || die "--client requires --name NAME"
    cn="craft-client-$name"
    key="$OUT/client-$name.key"; csr="$OUT/client-$name.csr"; crt="$OUT/client-$name.pem"; ext="$(mktemp)"
    gen_key "$key"
    openssl req -new -key "$key" -out "$csr" -subj "/CN=$cn" 2>/dev/null
    cat > "$ext" <<EOF
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
EOF
    sign "$csr" "$crt" "$ext"
    rm -f "$csr" "$ext"
    echo "client cert: $crt (CN=$cn)"
    echo "  CRAFT_CLIENT_CERT=$crt CRAFT_CLIENT_KEY=$key CRAFT_CA_CERT=$CA_CERT"
}

case "$MODE" in
    ca-only) make_ca ;;
    node)    ensure_ca; make_node "$NODE_ID" ;;
    client)  ensure_ca; make_client "$CLIENT_NAME" ;;
esac
