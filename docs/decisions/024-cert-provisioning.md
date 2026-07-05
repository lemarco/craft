# ADR 024: Cert provisioning — script + docs

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Medium open question **#5**: how users provision mTLS material for VPS deploys ([ADR 006](006-security.md)).

User chose **Option C — script + docs**.

## Decision

Ship **both**:

1. **`examples/certs/`** — runnable script(s) to generate dev/small-prod PKI
2. **`docs/certs.md`** — manual OpenSSL equivalent + env var reference + rotation notes

### Script (`examples/certs/generate.sh` or `craft-cert-gen` binary)

Generates:

| Artifact | Purpose |
|----------|---------|
| `ca.pem` / `ca.key` | Cluster CA (keep `ca.key` secure) |
| `node-{id}.pem` / `node-{id}.key` | Per-VPS mTLS (SAN includes `NodeId`) |
| `client-app.pem` / `client-app.key` | `RemoteClient` mTLS |

Usage:

```bash
./examples/certs/generate.sh --node-id 1 --out ./certs/node1
./examples/certs/generate.sh --node-id 2 --out ./certs/node2 --ca ./certs/ca.pem --ca-key ./certs/ca.key
./examples/certs/generate.sh --client --name my-app --out ./certs/client
```

Uses `rcgen` (Rust) or `openssl` in script — implementation choice at Phase 0; **document both paths** in `docs/certs.md`.

### Environment variables (production)

Document in `docs/certs.md`:

```bash
CRAFT_CA_CERT=/etc/craft/ca.pem
CRAFT_NODE_CERT=/etc/craft/node.pem
CRAFT_NODE_KEY=/etc/craft/node.key
CRAFT_CLIENT_CERT=/etc/craft/client.pem   # RemoteClient / split process
CRAFT_CLIENT_KEY=/etc/craft/client.key
```

### Docs must cover

- First VPS (create CA + node 1 cert)
- Joining VPS N (reuse CA, new node cert)
- Issuing client cert for app using `RemoteClient`
- **`insecure-dev`** for local sim only
- Rotation: reissue node cert, rolling restart (no auto-rotation in v1)
- Redis TLS (optional) — pointer only; user-managed ([ADR 021](021-actor-state-redis.md))

### Out of scope v1

- ACME, step-ca, cert-manager integration (defer)

## Related

- [006-security.md](006-security.md)
- [004-deployment-model.md](004-deployment-model.md)
