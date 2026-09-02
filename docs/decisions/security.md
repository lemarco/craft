# Transport security

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-07-05 — client path **mTLS required** in production

## Context

With [HTTP/3 for all wire traffic](wire-protocol.md), **TLS is mandatory** on the wire. Peer path already uses **mTLS**. User chose **client mTLS** as the production default for `POST /raft/v1/client/wire` — strongest access control for `RemoteClient` callers.

**Not in scope:** browser HTTPS to the user’s own web app — that uses **separate** TLS on the user’s HTTP server (443). Browsers never call `/client/wire` directly ([deployment-model](deployment-model.md)).

## Decision

### Peer RPC (`POST /raft/v1/peer/wire`, `/actor/*` node-to-node)

**mTLS required** in production.

- Node cert SAN / custom OID maps to `NodeId`
- Reject unknown or mismatched peer certs (fail closed)

### Client API (`POST /raft/v1/client/wire`)

**mTLS required** in production.

- Server presents cluster node cert (client verifies cluster CA)
- **Client must present a cert** signed by the same cluster CA (or designated client CA)
- Optional cert identity: `ClientId` in SAN for audit / ACL hooks
- Reject requests without valid client cert

```rust
// RemoteClient — production
RemoteClient::connect(addr, ClientTlsConfig {
    ca: cluster_ca,
    client_cert: my_client_cert,
    client_key: my_client_key,
})?;
```

### In-process client (`ClientHandle`)

**No TLS** — same process as `TrembitaCluster`; messages via `ractor`. This is the recommended path when the user’s HTTPS app and trembita run in **one binary** on the same VPS.

| Caller | Path | TLS |
|--------|------|-----|
| User handler → `ClientHandle` | in-process | None |
| Separate process → `RemoteClient` | HTTP/3 `/client/wire` | **mTLS** |
| Browser → user’s axum/actix HTTPS | user’s port 443 | User’s TLS (separate) |

### Development / tests

- **`insecure-dev` feature** (tests only): skip verification; `rcgen` self-signed material
- **`trembita-sim`:** in-memory transport; no TLS

Production examples document a **single cluster CA** issuing:

- **Node certs** (peer + server for `/client/wire`)
- **Client certs** (each app/service that uses `RemoteClient`)

Provide `dev/certs/` script: generate CA, node cert, client cert.

## Rust stack

- `rustls` + `quinn` for QUIC mTLS
- Cert loading via `rustls-pemfile` or embedded paths from env:

```bash
TREMBITA_CA_CERT=/etc/trembita/ca.pem
TREMBITA_NODE_CERT=/etc/trembita/node.pem
TREMBITA_NODE_KEY=/etc/trembita/node.key
TREMBITA_CLIENT_CERT=/etc/trembita/client.pem   # for RemoteClient
TREMBITA_CLIENT_KEY=/etc/trembita/client.key
```

## Consequences

**Positive**

- Same trust model on peer and client wire paths
- Only holders of client certs can call `/client/wire` on the network
- Aligns with VPS exposed to internet

**Negative**

- Must issue/revoke client cert per calling service (not per browser user)
- Split-process apps (HTTPS app ≠ trembita node) need cert provisioning

## Alternatives rejected

| Option | Why not |
|--------|---------|
| Server TLS only on client path | Weaker; user chose mTLS for production |

## Related

- [wire-protocol.md](wire-protocol.md)
- [client-and-routing.md](client-and-routing.md)
- [deployment-model.md](deployment-model.md)
- [cluster-membership.md#discovery](cluster-membership.md#discovery)
