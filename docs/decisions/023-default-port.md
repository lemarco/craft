# ADR 023: Default listen port — 7443/udp

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Medium open question **#4**: default QUIC/HTTP/3 listen port for `CraftCluster`.

User chose **`7443/udp`** as the fixed default.

## Decision

**Default listen address:** `0.0.0.0:7443` (UDP, HTTP/3).

All craft wire traffic on one listener ([ADR 010](010-wire-transport.md)): peer, client, join, actor routes.

### Configuration

| Source | Key | Default |
|--------|-----|---------|
| Builder | `.listen(addr)` | `0.0.0.0:7443` if omitted |
| Environment | `LISTEN_ADDR` | same |

```rust
CraftCluster::builder()
    // .listen("0.0.0.0:7443")  // optional — this is the default
    .spawn()
    .await?;
```

```bash
LISTEN_ADDR=0.0.0.0:18443  # override
```

Firewall docs: open **UDP 7443** (or chosen port) for peer and client mTLS ([ADR 006](006-security.md)).

## Related

- [protocol.md](../protocol.md)
- [010-wire-transport.md](010-wire-transport.md)
