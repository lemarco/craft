# Health / readiness — separate admin HTTP port

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Medium open question **#6**: how load balancers / ops check node health without speaking craft's HTTP/3 + postcard + mTLS wire ([wire-transport](wire-transport.md), [security](security.md)).

Chosen: **Option B — separate admin HTTP port**, so standard probes (curl, LB health checks, k8s if used later) work without QUIC or client certs.

## Decision

**Separate admin HTTP/1.1 listener**, default **`0.0.0.0:8080/tcp`**, distinct from the craft QUIC port (`7443/udp`, [default-port](default-port.md)).

### Endpoints

| Route | Meaning | 200 when |
|-------|---------|----------|
| `GET /health` | **Liveness** — process running | Always while process alive |
| `GET /ready` | **Readiness** — able to serve | Raft **member of cluster**, not draining, auto workers spawned ([auto-spawn-on-join](auto-spawn-on-join.md)) |
| `GET /metrics` | Prometheus metrics ([observability](observability.md)) | Always |
| `GET /introspect/*` | Cluster/actor introspection ([observability](observability.md)) | Always |
| `GET /dashboard` | Live monitoring UI ([observability](observability.md)) | Always |

Responses: plain HTTP, JSON body (admin surface is **not** the postcard hot path — human/tooling readable here is fine).

```
GET /ready
200 {"node_id":2,"role":"follower","member":true,"draining":false,"workers":["orders"]}
503 {"node_id":2,"member":false,"reason":"joining"}
```

### Readiness states

| State | `/ready` |
|-------|----------|
| Joining, not yet in Raft config | `503` |
| Member, worker spawned | `200` |
| Draining / leaving ([drain-timeout](drain-timeout.md)) | `503` |
| Leader or follower, healthy | `200` |

### Configuration

| Source | Key | Default |
|--------|-----|---------|
| Builder | `.admin_listen(addr)` | `0.0.0.0:8080` |
| Environment | `CRAFT_ADMIN_ADDR` | same |
| Environment | `CRAFT_ADMIN_TLS_CERT` / `CRAFT_ADMIN_TLS_KEY` | optional admin HTTPS (both required) |
| Disable | `.admin_disabled()` / `CRAFT_ADMIN_ADDR=off` | admin off |

### Security

- Admin port carries **no consensus / client data** — only health + metrics.
- Default plain HTTP for LB simplicity; **bind to private interface** or firewall recommended.
- Optional TLS via `.admin_tls(cert, key)` or `CRAFT_ADMIN_TLS_*` on `craft-node` — **server-only** PEM (no client certs); suitable for bare VPS HTTPS or Ingress TLS termination upstream.
- No mTLS requirement (unlike `/client/wire`), since no sensitive operations.

### Out of scope v1

- Admin mutation endpoints (trigger leave, scale) — CLI/`RemoteClient` handles those, not admin HTTP.

## Consequences

**Positive**

- Works with any LB / uptime probe (plain HTTP/1.1)
- Keeps `7443` craft-only and mTLS-clean
- Natural `/metrics` home

**Negative**

- Extra port + firewall rule
- Must document "do not expose admin publicly"

## Related

- [default-port.md](default-port.md)
- [security.md](security.md)
- [auto-spawn-on-join.md](auto-spawn-on-join.md)
- [drain-timeout.md](drain-timeout.md)
