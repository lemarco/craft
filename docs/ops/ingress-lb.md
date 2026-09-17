# Ingress and load balancing (product recipe)

**B-30** — how to put **public HTTP/WebSocket** in front of **N homogeneous trembita processes** on VPS or bare metal. This is **your** LB/DNS/firewall setup; trembita does not ship an orchestrator ([deployment-model](../decisions/deployment-model.md)).

**Same binary everywhere:** each backend listens on [`TREMBITA_LISTEN`](../env.md#trembita_listen) (default `0.0.0.0:443`) — **TCP** for product + ops HTTP, **UDP** for QUIC cluster wire on the **same port number** ([wire-protocol](../decisions/wire-protocol.md)).

## Target architecture

```text
                    ┌─────────────────────────────┐
  Clients ──HTTPS──►│  Edge: DNS / anycast /      │
  (browser, API)    │  floating IP / reverse proxy │
                    └──────────────┬──────────────┘
                                   │ TCP :443 (HTTP/WS)
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
         trembita VPS 1      trembita VPS 2      trembita VPS 3
         TREMBITA_LISTEN      same                same
         /health /ready        product APIs        capabilities
```

**Cluster wire (UDP)** between members should reach **each node’s real IP** (or private mesh), not only the VIP — join, Raft, and mTLS peer traffic use QUIC ([certs.md](../certs.md)). The edge LB fronts **client-facing TCP**; peer UDP is usually a **separate firewall allowlist** (all nodes ↔ all nodes on the listen port).

## Backend pool

| Rule | Detail |
|------|--------|
| Homogeneous | Same release artifact, same env **except** per-node `TREMBITA_DATA_DIR` and assigned `node-id` |
| Listen | `TREMBITA_LISTEN=0.0.0.0:443` (or `:8443` in lab) on every node |
| Health | Register **`GET /health`** (liveness) and **`GET /ready`** (readiness) on the **TCP** bind ([production-runbook](production-runbook.md)) |
| Drain | Rolling upgrade: coordinator + `/ready` ≠ 200 while draining ([rolling-upgrade](rolling-upgrade.md)) |

### Health check semantics

| Path | Use on LB | Meaning |
|------|-----------|---------|
| [`GET /health`](../decisions/wire-protocol.md#ops-http-tcp-on-trembita_listen) | **Liveness** (optional) | Process up; always 200 while running |
| [`GET /ready`](../decisions/wire-protocol.md#ops-http-tcp-on-trembita_listen) | **Readiness (required)** | Member of cluster, not draining — **remove from pool when non-200** |

Prefer **readiness-only** pools for API traffic so joining or draining nodes do not receive product requests.

Example (HAProxy TCP mode with HTTP check):

```haproxy
backend trembita_http
    option httpchk GET /ready
    http-check expect status 200
    default-server inter 3s fall 3 rise 2
    server node1 10.0.0.11:443 check ssl verify none
    server node2 10.0.0.12:443 check ssl verify none
    server node3 10.0.0.13:443 check ssl verify none
```

(Terminate TLS at HAProxy or use `ssl` to backends with `TREMBITA_HTTP_TLS_*` — see below.)

## Edge patterns (pick one)

| Pattern | When | Notes |
|---------|------|-------|
| **DNS A/AAAA round-robin** | Simple, low ops | Clients retry another IP; combine with short TTL + `/ready` per node behind separate monitors |
| **Floating IP / anycast** | One stable client IP | Failover moves VIP; backends still need individual UDP reachability for cluster |
| **Reverse proxy (nginx, Caddy, Envoy, HAProxy)** | TLS termination, WAF, rate limits | Terminate HTTPS at edge; proxy HTTP/1.1 + WebSocket to backends |
| **Cloud LB (L4/L7)** | Managed health checks | Map provider “health check URL” to `http://<node-ip>:443/ready` on the **node**, or through LB if using target groups |

trembita does **not** maintain provider-specific modules — configure the provider’s “HTTP health check on `/ready`” and TCP forward to `TREMBITA_LISTEN`.

## TLS and mTLS

| Path | Typical setup |
|------|----------------|
| **Browser / public API** | TLS at edge **or** [`TREMBITA_HTTP_TLS_CERT` / `KEY`](../env.md) on each node (`TREMBITA_HTTP_TLS_*`) |
| **Node ↔ node (QUIC wire)** | mTLS from [`TREMBITA_CERT_DIR`](../env.md#trembita_cert_dir) — independent of edge TLS |
| **Product auth** | [`GATEWAY_TOKEN`](../env.md#gateway_token), [`GatewayIdentity`](../decisions/gateway-identity.md), cluster session cookies ([gateway-cluster-auth](../decisions/gateway-cluster-auth.md)) |

**Passthrough (TCP :443 → backend :443):** edge does not terminate; each node presents its own cert (or shared wildcard). Simpler for QUIC+HTTP same port, harder at CDN.

**Terminate at edge:** edge HTTPS → plain HTTP or re-encrypt to backends; set `TREMBITA_HTTP_TLS_*` only if clients hit nodes directly (e.g. admin paths).

## QUIC vs HTTP at the edge

| Traffic | Protocol | Through LB? |
|---------|----------|-------------|
| **Clients → product HTTP/WS** | TCP on `TREMBITA_LISTEN` | **Yes** — primary ingress recipe |
| **Nodes ↔ nodes (Raft, join)** | UDP QUIC on same port number | **Usually no** — direct node IPs / private network |
| **Optional QUIC-only clients** | UDP | Needs **L4 UDP** LB with session affinity; most teams use **TCP HTTP** at edge and keep UDP for inter-node only |

Do not block **UDP** between cluster members on the listen port ([production-runbook](production-runbook.md)).

## Session stickiness (WebSocket / realtime)

| Need | Approach |
|------|----------|
| **Sticky WebSocket** to capability host | LB **cookie or IP affinity** to one backend for the WS lifetime ([realtime-sessions](../scenarios/realtime-sessions.md)) |
| **HTTP session cookie after login** | [`TREMBITA_GATEWAY_SESSION_SECRET`](../env.md#trembita_gateway_session_secret) — **any** backend validates; LB need not stick HTTP if only signed cookies + stateless gateway |
| **Sticky compute** | [`Route::Session`](../scenarios/realtime-sessions.md) + directory — after WS lands on a node, trembita pins to a cap host |

For realtime: enable **WS upgrade affinity** on the LB; for REST after cluster login cookies, round-robin is fine when using [cluster session auth](../decisions/gateway-cluster-auth.md).

## Splitting “ingress-only” nodes (optional)

You may run some VPSes **without** local job consumers (omit `.jobs()` / heavy `.capabilities` queues) to spare CPU for gateway — still the **same binary**, different manifest/env ([workload governor](../decisions/workload-governor.md)). LB pool can include only ingress-capable nodes for **public** routes while workers scale on a private pool; both join the same cluster via `TREMBITA_JOIN_SEEDS`.

## Firewall checklist

- [ ] **TCP** `TREMBITA_LISTEN` from LB (or public) to backend pool
- [ ] **UDP** `TREMBITA_LISTEN` **between all cluster members** (not only via VIP)
- [ ] Restrict `/metrics`, `/dashboard`, `/introspect/*` to admin network ([production-runbook](production-runbook.md))
- [ ] `GATEWAY_TOKEN` / session secrets set in production ([env.md](../env.md))

## Automated regression (B-30)

CI exercises the LB-facing HTTP contract without an external balancer:

| Area | Location |
|------|----------|
| `/ready` pool vs `/health` liveness | `trembita-http/src/ops_routes.rs` (`ingress_lb_*` unit scenarios) |
| `Readiness::is_ready` | `trembita-dashboard/src/views.rs` |
| 3-node backends + concurrent `/ready` polls | `trembita/src/integration/ingress_lb.rs` |
| `DefaultGatewayApis::ops_only` on product gateway | `trembita/tests/ingress_lb_ops.rs` |

Run locally:

```bash
./scripts/test-fast.sh -p trembita-http --lib
./scripts/test-fast.sh -p trembita-dashboard --lib
./scripts/test-fast.sh -p trembita --lib ingress_lb
./scripts/test-fast.sh -p trembita --test ingress_lb_ops
```

## Verify after cutover

1. Each backend: `curl -sf "https://<node>/ready"` (or via LB).
2. Product smoke: enqueue job, cap invoke, or app-specific path through **LB VIP**.
3. Add one node — LB discovers it when `/ready` is 200 ([cluster-elasticity](../decisions/cluster-elasticity.md)).
4. Rolling upgrade — pool shrinks while `/ready` fails on draining nodes ([rolling-upgrade](rolling-upgrade.md)).

Wave index (B-28–B-32): [status § Product scale wave](../status.md#product-scale-wave-b-28b32).

## Related

- [production-runbook.md](production-runbook.md) — VPS checklist, observability
- [getting-started.md](../getting-started.md) — product boot
- [env.md](../env.md) — `TREMBITA_LISTEN`, TLS, auth
- [deployment-model](../decisions/deployment-model.md) — library-first, no mandatory orchestrator
