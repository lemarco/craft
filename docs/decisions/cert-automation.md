# mTLS cert automation (step-ca, cert-manager, hot reload)

**Status:** Accepted  
**Date:** 2026-08-27

## Context

[cert-provisioning](cert-provisioning.md) ships manual PKI (`examples/certs/generate.sh`,
`docs/certs.md`) and documents **rolling restart** rotation. [future-work-and-risks](future-work-and-risks.md)
R6 notes the operational burden and defers ACME/step-ca/cert-manager integration.

Operators now want **automatic issuance and renewal** without restarting the whole
cluster on every cert-manager or step-ca renewal. Public ACME (Let's Encrypt) is
**out of scope**: craft mTLS requires a **private cluster CA**, `serverAuth` +
`clientAuth` EKU, and a SAN/CN of `craft-node-<NodeId>` ([security](security.md)).

## Decision

### 1. Issuance stays external; craft applies material from disk

craft **does not embed an ACME client**. CAs and renewers run outside the binary:

| Environment | Issuer | Renewal |
|-------------|--------|---------|
| **VPS / docker-compose** | [step-ca](https://smallstep.com/docs/step-ca/) (internal PKI + ACME/JWK provisioners) | `step ca renew` cron/sidecar rewrites PEM files |
| **Kubernetes** | [cert-manager](https://cert-manager.io/) `Certificate` + `ClusterIssuer` / CA issuer | cert-manager renews Secret; kubelet syncs files into the pod |

Issued certs must match the existing contract ([cert-provisioning](cert-provisioning.md)):

- Node: CN **and** DNS SAN = `craft-node-<id>`; EKU `serverAuth,clientAuth`
- Client (`RemoteClient`): EKU `clientAuth`; CN convention `craft-client-<name>`
- Trust anchor: `ca.pem` (may contain multiple CA certs during CA migration)

Env vars unchanged: `CRAFT_NODE_CERT`, `CRAFT_NODE_KEY`, `CRAFT_CA_CERT`.

### 2. Hot reload in `craft-net` (no full process restart)

When PEM paths are configured, the runtime **reloads TLS** without exiting:

1. Re-read cert/key/CA from disk (`rustls-pemfile`).
2. Rebuild `quinn` server + client configs ([`tls::server_config`](../../crates/craft-net/src/tls.rs) / [`client_config`](../../crates/craft-net/src/tls.rs)).
3. Apply server config via `Endpoint::set_server_config` (new handshakes only).
4. Swap client config and **evict** cached QUIC connections so outbound dials pick up the new identity.

Existing QUIC connections keep the old TLS session until they close naturally;
consensus heartbeats reconnect quickly enough for rolling renewal.

**Triggers:**

| Trigger | Default | Notes |
|---------|---------|-------|
| **File poll** | on when [`PemSecurity`](../../crates/craft/src/security.rs) is used | Compare `(mtime, size)` fingerprint every `CRAFT_CERT_WATCH_SECS` (default **60**) |
| **`SIGHUP`** | on when PEM paths set | Manual / hook after `step ca renew` |
| Admin HTTP POST | **no** | Admin port stays read-only ([health-admin-port](health-admin-port.md)) |

Builder API: [`Security::from_pem_files`](../../crates/craft/src/security.rs) →
[`PemSecurity`](../../crates/craft/src/security.rs); pass to
[`CraftClusterBuilder::start_quic`](../../crates/craft/src/builder.rs). Optional
[`.cert_watch(period)`](../../crates/craft/src/builder.rs) enables polling.

### 3. Rolling order (operational)

Automatic file reload removes the need to **restart** a node for leaf cert
renewal, but operators should still avoid simultaneous reload on every member:

1. Reload **followers** first (or let cert-manager renew them on staggered schedules).
2. Reload the **leader** last — `CertReloadHandle::reload_now` returns
   `CertReloadError::ReloadLeaderLast` when the node is leader unless
   `ReloadOpts { allow_leader: true }` is set.

For **CA rotation**, keep the dual-CA bundle in `ca.pem` until every node trusts
the new CA, then roll leaf certs, then trim the old CA ([docs/certs.md](../certs.md)).

### 4. Deliverables

| Path | Purpose |
|------|---------|
| `examples/step-ca/` | docker-compose + bootstrap issuing node certs; renewal + `SIGHUP` demo |
| `deploy/kubernetes/cert-manager/` | `ClusterIssuer` + per-ordinal `Certificate` CRs + StatefulSet volume notes |
| `docs/certs.md` § Automation | Operator runbook |
| `craft-net` `pem` + reload | Load + apply |
| `craft` `PemSecurity` / `CertReloadHandle` | Facade + `craft-node` wiring |

### Out of scope

- Public ACME / Let's Encrypt for craft wire identities
- cert-manager **inside** the Rust library
- Mutating admin endpoints for reload
- Automatic leader step-down before reload (operators stagger reloads)

## Consequences

- cert-manager and step-ca integrate without new wire protocol or security changes
- Renewals become **apply-on-disk + poll/SIGHUP** instead of mandatory pod restart
- R6 mitigation upgraded from “script + docs” to “script + automation examples + hot reload”
- Slightly more runtime complexity in `craft-net` (config swap + pool eviction)

## Related

- [security.md](security.md) — mTLS policy
- [cert-provisioning.md](cert-provisioning.md) — v1 manual PKI
- [health-admin-port.md](health-admin-port.md) — admin stays read-only
- [future-work-and-risks.md](future-work-and-risks.md) — R6
- [../certs.md](../certs.md) — operator guide
