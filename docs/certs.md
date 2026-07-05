# TLS certificates for craft (VPS)

See [ADR 024](decisions/024-cert-provisioning.md). Full guide — to be expanded during Phase 0.

## Quick start (script)

```bash
# Generate CA + node 1
./examples/certs/generate.sh --init-ca --node-id 1 --out ./certs/node1

# Node 2 (same CA)
./examples/certs/generate.sh --node-id 2 --out ./certs/node2 \
  --ca ./certs/node1/ca.pem --ca-key ./certs/node1/ca.key

# Client cert for RemoteClient
./examples/certs/generate.sh --client --name my-app --out ./certs/client \
  --ca ./certs/node1/ca.pem --ca-key ./certs/node1/ca.key
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `CRAFT_CA_CERT` | Cluster CA (verify peers/clients) |
| `CRAFT_NODE_CERT` | This VPS node certificate |
| `CRAFT_NODE_KEY` | Node private key |
| `CRAFT_CLIENT_CERT` | App client cert (split process) |
| `CRAFT_CLIENT_KEY` | Client private key |

## Manual OpenSSL

_(Step-by-step commands added when script is implemented.)_

## Rotation

Reissue certs with same CA; rolling restart nodes. No automatic rotation in v1.
