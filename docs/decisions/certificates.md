# Certificates & mTLS provisioning

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-08-28 — merged cert-provisioning and cert-automation

## Context

Users need mTLS material for VPS deploys ([security](security.md)). v1 ships manual PKI; operators also want **automatic issuance and renewal** without full process restarts. Public ACME (Let's Encrypt) is **out of scope** — trembita mTLS requires a private cluster CA with `serverAuth` + `clientAuth` EKU and SAN/CN of `trembita-node-<NodeId>`.

## Manual provisioning (v1 baseline)

Ship **both**:

1. **`dev/certs/`** — runnable script(s) to generate dev/small-prod PKI
2. **`docs/certs.md`** — manual OpenSSL equivalent + env var reference + rotation notes

### Script output

| Artifact | Purpose |
|----------|---------|
| `ca.pem` / `ca.key` | Cluster CA |
| `node-{id}.pem` / `node-{id}.key` | Per-VPS mTLS (SAN includes `NodeId`) |
| `client-app.pem` / `client-app.key` | `RemoteClient` mTLS |

### Environment variables

```bash
TREMBITA_CA_CERT=/etc/trembita/ca.pem
TREMBITA_NODE_CERT=/etc/trembita/node.pem
TREMBITA_NODE_KEY=/etc/trembita/node.key
TREMBITA_CLIENT_CERT=/etc/trembita/client.pem
TREMBITA_CLIENT_KEY=/etc/trembita/client.key
```

Docs cover: first VPS (create CA), joining VPS N, client cert for `RemoteClient`, `insecure-dev` for local sim, rolling restart rotation (no auto-rotation in manual-only mode).

## Automation & hot reload (landed)

### Issuance stays external

trembita **does not embed an ACME client**. CAs and renewers run outside the binary:

| Environment | Issuer | Renewal |
|-------------|--------|---------|
| **VPS / docker-compose** | step-ca | `step ca renew` cron/sidecar rewrites PEM files |

Issued certs must match the manual contract above. Env vars unchanged.

### Hot reload in `trembita-net`

When PEM paths are configured, reload TLS without exiting:

1. Re-read cert/key/CA from disk.
2. Rebuild `quinn` server + client configs.
3. Apply server config via `Endpoint::set_server_config`.
4. Swap client config and evict cached QUIC connections.

| Trigger | Default |
|---------|---------|
| **File poll** | on when `PemSecurity` used; every `TREMBITA_CERT_WATCH_SECS` (default 60) |
| **`SIGHUP`** | on when PEM paths set |

Builder: `Security::from_pem_files` → `PemSecurity`; `.cert_watch(period)` enables polling. Admin HTTP stays read-only — no POST reload endpoint.

### Rolling order

1. Reload **followers** first (stagger renewal schedules).
2. Reload **leader** last — `CertReloadHandle::reload_now` returns `ReloadLeaderLast` unless `allow_leader: true`.
3. CA rotation: dual-CA bundle in `ca.pem` until all nodes trust new CA, then roll leaf certs, then trim old CA.

### Deliverables

| Path | Purpose |
|------|---------|
| `dev/step-ca/` | docker-compose + bootstrap + renewal demo |
| `docs/certs.md` § Automation | Operator runbook |
| `trembita-net` pem + reload | Load + apply |
| `trembita` `PemSecurity` / `CertReloadHandle` | Facade + `trembita-node` wiring |

## Consequences

**Positive:** Script + docs for day-one deploy; step-ca integrates without wire changes; renewals become apply-on-disk + poll/SIGHUP.

**Negative:** Manual path still requires rolling restart without hot reload; slightly more runtime complexity (config swap + pool eviction).

## Related

- [security.md](security.md) — mTLS policy
- [wire-protocol.md](wire-protocol.md) — admin port stays read-only
- [../certs.md](../certs.md) — operator guide
