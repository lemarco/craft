# ADR 006: Transport security

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-07-05 — client path **mTLS required** in production

## Context

With [HTTP/3 for all wire traffic](010-wire-transport.md), **TLS is mandatory** on the wire. Peer path already uses **mTLS**. User chose **client mTLS** as the production default for `POST /raft/v1/client/wire` — strongest access control for `RemoteClient` callers.

**Not in scope:** browser HTTPS to the user’s own web app — that uses **separate** TLS on the user’s HTTP server (443). Browsers never call `/client/wire` directly ([ADR 004](004-deployment-model.md)).

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

**No TLS** — same process as `CraftCluster`; messages via `ractor`. This is the recommended path when the user’s HTTPS app and craft run in **one binary** on the same VPS.

| Caller | Path | TLS |
|--------|------|-----|
| User handler → `ClientHandle` | in-process | None |
| Separate process → `RemoteClient` | HTTP/3 `/client/wire` | **mTLS** |
| Browser → user’s axum/actix HTTPS | user’s port 443 | User’s TLS (separate) |

### Development / tests

- **`insecure-dev` feature** (tests only): skip verification; `rcgen` self-signed material
- **`craft-sim`:** in-memory transport; no TLS

Production examples document a **single cluster CA** issuing:

- **Node certs** (peer + server for `/client/wire`)
- **Client certs** (each app/service that uses `RemoteClient`)

Provide `examples/certs/` script: generate CA, node cert, client cert.

## Rust stack

- `rustls` + `quinn` for QUIC mTLS
- Cert loading via `rustls-pemfile` or embedded paths from env:

```bash
CRAFT_CA_CERT=/etc/craft/ca.pem
CRAFT_NODE_CERT=/etc/craft/node.pem
CRAFT_NODE_KEY=/etc/craft/node.key
CRAFT_CLIENT_CERT=/etc/craft/client.pem   # for RemoteClient
CRAFT_CLIENT_KEY=/etc/craft/client.key
```

## Consequences

**Positive**

- Same trust model on peer and client wire paths
- Only holders of client certs can call `/client/wire` on the network
- Aligns with VPS exposed to internet

**Negative**

- Must issue/revoke client cert per calling service (not per browser user)
- Split-process apps (HTTPS app ≠ craft node) need cert provisioning

## Alternatives rejected

| Option | Why not |
|--------|---------|
| Server TLS only on client path | Weaker; user chose mTLS for production |

## Related

- [010-wire-transport.md](010-wire-transport.md)
- [002-client-api.md](002-client-api.md)
- [004-deployment-model.md](004-deployment-model.md)
- [007-discovery.md](007-discovery.md)
