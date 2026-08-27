# Open implementation topics

All **architecture decision records (ADRs 001–019)** are **accepted**.

**All priority v1 topics decided.** Remaining items are **medium** (implement-time) or **low** (defer). Items below are **implementation / ops** choices not yet locked — discuss before or during Phase 0–5.

## Priority — affects v1 design

### 1. ~~Join before full membership (Phase 6)~~ — **Decided**

**[ADR 016](decisions/016-membership-early.md):** **Full joint-consensus membership in early phases** (not deferred). `JOIN_ADDR` dynamic join is v1; implemented in `craft-core` + `/cluster/join` + supervisor integration.

---

### 2. ~~Auto-provision worker on new VPS join~~ — **Decided**

**[ADR 015](decisions/015-auto-spawn-on-join.md):** Framework **auto-spawns** workers declared via `.auto_workers([...])` when a node joins (and on seed startup). `ClusterSupervisor` reconciles 1 worker per VPS in production.

---

### 3. ~~Client TLS: mTLS or server-only?~~ — **Decided**

**[ADR 006](decisions/006-security.md):** **mTLS required** on `/client/wire` in production (same CA story as peers). **`ClientHandle`** in-process bypasses TLS. User’s **browser HTTPS app** uses separate TLS on their web server — not craft client mTLS.

---

### 4. ~~Join RPC shape~~ — **Decided**

**[ADR 017](decisions/017-join-rpc.md):** `POST /raft/v1/cluster/join` handshake; leader proposes **joint-consensus membership** in Raft log.

---

### 5. ~~`cluster(name)` routing default~~ — **Decided**

**[ADR 019](decisions/019-cluster-routing.md):** **`send`** = round-robin; **`send_keyed`** = consistent hash on key. Both in v1.

---

### 6. ~~ClusterSupervisor coordination~~ — **Decided**

**[ADR 018](decisions/018-supervisor-leader.md):** **Leader only** runs cluster-wide reconcile (auto-spawn, `scale_cluster`, migration planning). Non-leaders forward; local dev spawn unchanged.

---

## Medium — can decide during implementation

| Topic | Notes |
|-------|--------|
| **~~Version skew on join~~** | **[ADR 020](decisions/020-join-version-skew.md): hard reject (exact protocol + exact app semver)** |
| **~~Stateful actor crash~~** | **[ADR 021](decisions/021-actor-state-redis.md): Redis / external `ActorStateStore`; Raft SM for consensus only** |
| **~~Graceful drain timeout~~** | **[ADR 022](decisions/022-drain-timeout.md): default 60s, `CRAFT_DRAIN_TIMEOUT` / builder** |
| **~~Default HTTP port~~** | **[ADR 023](decisions/023-default-port.md): default `0.0.0.0:7443/udp`, overridable** |
| **~~Cert provisioning story~~** | **[ADR 024](decisions/024-cert-provisioning.md): `examples/certs/` script + [certs.md](certs.md)** |
| **~~Health / readiness HTTP~~** | **[ADR 025](decisions/025-health-admin-port.md): separate admin HTTP `:8080` — `/health`, `/ready`, `/metrics`** |

## Low — **decided: deferred** ([ADR 027](decisions/027-future-work-and-risks.md))

All low items recorded as intentional deferrals with known risks in [ADR 027](decisions/027-future-work-and-risks.md):

| Topic | Stance |
|-------|--------|
| Follower reads | **Done** — see [ADR 005](decisions/005-read-consistency.md) |
| Lease reads | Deferred |
| Gossip discovery | Deferred |
| Dev-only JSON wire | Likely skip |
| Write sharding / multi-Raft | Deferred — primary future write-scaling path (R1) |
| K8s / cloud integrations | Deferred — enabled externally via admin port |
| Traffic priorities on QUIC | **Cheap safeguard adopted** (peer connection isolation, R2); full tuning deferred |

## Status

**All open questions resolved.** ADRs **001–027** accepted — see [README.md](README.md). Design ready for Phase 0.
