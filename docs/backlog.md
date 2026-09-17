# Backlog

**Open work only.** Shipped capabilities: [status.md](status.md). Shipped epic history: [archive/backlog-shipped.md](archive/backlog-shipped.md) (B-01…B-27), [archive/backlog-wave-b28-b32.md](archive/backlog-wave-b28-b32.md) (B-28…B-32). **B-33…B-43** index: [status § Product scale wave](status.md#product-scale-wave-b-28b32). Design rationale: [decisions/](decisions/).

When an item ships, remove its row here, update [status.md](status.md), and move planning text to `docs/archive/` if it is no longer needed in live docs.

---

## Open work

**Pre-1.0 policy:** breaking API/default changes are OK when they match product intent. Optional cap-store backends ship via `trembita` features — see [status.md](status.md).

**Priority hint:** P0 → P1 → P2 within this table (top first). Next epic id after this table ships: **B-55**. **1.0 stabilization** (API freeze, Jepsen, semver) — **not** listed here; see [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md) when scheduling that program explicitly.

| Id | Pri | Item | Status | Notes |
|----|-----|------|--------|-------|
| **B-44** | P1 | **Coordination closed-loop** | 📋 | Leader policy on top of B-32/B-37: auto-shard / [`add_raft_groups`](../crates/trembita/src/app/runtime.rs) from backlog/SLO signals; **hard ceilings** + introspect “why not scaling”. ADR touch: [multi-raft](decisions/multi-raft.md), [job-queue](decisions/job-queue.md). |
| **B-45** | P1 | **Production deploy pack** | 📋 | `deploy/` + [production-runbook](ops/production-runbook.md): **systemd** unit templates, seed vs learner env matrix, certs, LB ([ingress-lb](ops/ingress-lb.md)), rolling upgrade hooks ([upgrade-coordinator](decisions/upgrade-coordinator.md)). VPS / bare metal only — no orchestrator charts. |
| **B-46** | P2 | **Gateway session store adapters** | 📋 | Optional **`GatewaySessionStore` → Postgres** (separate crate or feature), composable with B-40 [`SessionIssuer` / `SessionVerifier`](decisions/gateway-cluster-auth.md#b-40--logic--storage-split) — not the cap-store registry path. |
| **B-47** | P2 | **OAuth prod hardening** | 📋 | [`trembita-gateway-auth`](../crates/trembita-gateway-auth/): PKCE defaults, redirect allowlist, production wiring doc + harden [`examples/oauth-gateway`](../examples/oauth-gateway/). Rotation stays [runbook § B-40](ops/production-runbook.md#gateway-session-rotation-b-40). |
| **B-48** | P1 | **Backup / restore / DR** | 📋 | Runbook + supported procedure for **`TREMBITA_DATA_DIR`** (Raft + redb), cert material, and **safe restore/join** after node loss. Document what is *not* automatic; optional ops script or `trembita doctor` preflight checks. Depends on [B-45](backlog.md) env layout. |
| **B-49** | P1 | **Rolling upgrade proof** | 📋 | End-to-end **two-version binary** story on multi-node lab: drain, coordinator / cluster upgrade routes, re-join, session + queue continuity. Extends B-45 runbook with **automated regression** (sim or e2e lane; document `run-heavy` if needed). |
| **B-54** | P1 | **CI cluster upgrade operator** | 📋 | **External one-shot operator** (not SSH fleet loop): preflight via [B-43](backlog.md) / `/ready` / version skew; **`POST /cluster/upgrade/desired`** + poll **`GET /cluster/upgrade`**; timeouts + clear failure output; post smoke. In-cluster leader grant stays [upgrade-coordinator](decisions/upgrade-coordinator.md). Ship as `trembita-ops upgrade` or CLI subcommand; scaffold opt-in for `UpgradeApi` + coordinator. **After** B-49 proves self-update; complements B-45 deploy pack. |
| **B-50** | P2 | **CI local elastic smoke** | 📋 | Optional CI job (MR label **`run-heavy`**): `local-cluster` **or** slim script mirroring [B-42](backlog.md) — 4th joiner + `/ready` + session smoke; faster feedback than full [`elastic_lb.sh`](../e2e/elastic_lb.sh) when B-42 ships. |
| **B-51** | P2 | **Ops observability layer** | 📋 | Map [B-43](backlog.md) / join / queue / R3 signals to **OTLP metrics + documented alert thresholds** (join stuck, backlog depth, directory merge lag). Reuse existing runtime OTLP; product-facing metric names in [env.md](env.md) / runbook. |
| **B-52** | P2 | **Domain DX patterns** | 📋 | Scaffold + docs for common app patterns: **transactional outbox**, job **idempotency** with `require_store`, cap **consensus helpers** usage — reduce “how do I do this in Raft?” without new runtime primitives. Touch [capability-dx](decisions/capability-dx.md), [structural-limits](scenarios/structural-limits.md), optional `trembita new` samples. |
| **B-53** | P2 | **Backlog / coverage hygiene** | 📋 | Add `archive/backlog-wave-b33-b41.md` (mirror [B-28–B-32 archive](archive/backlog-wave-b28-b32.md)); align [testing-coverage.md](testing-coverage.md) shipped index **B-33…B-41**; fix cross-links from backlog header when archive exists. |

### Deferred decisions (ADR when epic starts)

| Topic | Options | Current lean |
|-------|---------|--------------|
| Coordination profile | Env-only vs `TrembitaConfigure` | **Shipped B-37** — both via [`CoordinationGrowthPreset`](../crates/trembita/src/configure.rs) |
| Closed-loop ceilings | Env-only vs configure API | **`TREMBITA_COORDINATION_MAX_*`** env with conservative defaults; override in `TrembitaConfigure` if B-44 needs it |
| Ops cockpit UI | Dashboard page vs JSON only | **JSON introspect first** (B-43); reuse [trembita-dashboard](../crates/trembita-dashboard/) only if a page adds clear value |
| Backup scope | Hot copy vs snapshot API | **Document supported manual procedure first** (B-48); automated snapshot only if ops demand it |
| CI upgrade binary | `trembita-ops` vs `trembita-cli` release | **`trembita-ops upgrade`** (or dedicated bin in workspace) — CI-friendly, no debug `dev` deps |
| Upgrade without self-update API | SSH/systemd manual roll | **Document in B-45**; B-54 **fails fast** if `/cluster/upgrade` not wired |
| 1.0 program | When to schedule | **Explicit decision only** — not part of B-42…B-54; track in [public-api-1.0](decisions/public-api-1.0.md) / [jepsen-1.0](decisions/jepsen-1.0.md) |

**How to update:** pick an id; set 🚧 while working; on release update [status.md](status.md); reference GitLab issues as `#<number>` in commits/MRs.

**Related:** [status.md](status.md) · [scenarios/README.md](scenarios/README.md) · [structural-limits](scenarios/structural-limits.md)
