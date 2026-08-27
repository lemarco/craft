# Tier 2 — production reliability

**Status:** Accepted  
**Date:** 2026-08-27

## Context

Post-v1 deferrals from [future-work-and-risks](future-work-and-risks.md) and ops
gaps identified for bare-metal / VPS deployments (distinct from K8s Ingress +
sidecar patterns already supported).

## Decision

Land six production-oriented capabilities:

| Feature | Implementation |
|---------|----------------|
| **Reachability tuning + hysteresis** | `ReachabilityConfig`, `CraftClusterBuilder::reachability()`, ack-window latch in `craft-core` |
| **Phi-accrual detector** | `FailureDetectorKind::PhiAccrual` (Haystack-style φ on heartbeat inter-arrivals) |
| **Snapshot backup / restore** | `craft-ops` CLI: local gzip-tar export/import + `s3://` / `gs://` / `file://` push/pull via opendal |
| **Rolling wire upgrade (N/N−1)** | `MIN_COMPATIBLE_PROTOCOL_VERSION` + `protocol_version_compatible()` on join, leave, and wire decode |
| **Admin TLS** | `AdminServer::serve_tls`, builder `.admin_tls()`, `CRAFT_ADMIN_TLS_*` on `craft-node` |
| **Jepsen-lite gate** | `e2e/linearizability.sh` — craft-sim checker sweep; docker phase runs `craft-e2e-client` (concurrent QUIC inc/read + `craft_sim::History` checker) before/after partition chaos |

`app_version` join skew remains **exact match** (state-machine safety); only
**protocol/wire** accepts a compatibility band.

## Consequences

**Positive**

- Supervisor reconcile flaps less under jitter (hysteresis + tunable window).
- Documented path to restore a cluster from object storage.
- Rolling deploys can stagger node restarts without wire hard-reject.
- Admin HTTPS on VPS without a reverse proxy.

**Negative / follow-ups**

- Partition chaos during concurrent QUIC load (not just before/after) remains
  future work.
- `craft-ops` object URIs use standard cloud SDK env vars (AWS/GCP); no vault integration.

## Related

- [liveness-vs-membership.md](liveness-vs-membership.md)
- [join-version-skew.md](join-version-skew.md)
- [health-admin-port.md](health-admin-port.md)
- [read-consistency.md](read-consistency.md)
