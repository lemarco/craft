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
| [`GET /ready`](../decisions/wire-protocol.md#ops-http-tcp-on-trembita_listen) | **Readiness (required)** | **`join_phase`: `pool_ready`** (voters + caught-up learners with auto-hosts) — **503 until then** ([cluster-elasticity § B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35)) |

Prefer **readiness-only** pools for API traffic so joining or draining nodes do not receive product requests.

### Join readiness pipeline (B-35)

Elastic joiners progress through **`join_phase`** before **`GET /ready`** returns **200**. Configure the LB to **`GET /ready`** (not `/health`) for pool membership.

| `join_phase` | LB should |
|--------------|-----------|
| `awaiting_membership`, `catching_up`, `awaiting_hosts` | **503** — keep backend **out** of rotation |
| `pool_ready` | **200** — accept product HTTP / WebSocket (subject to drain — see [rolling-upgrade](rolling-upgrade.md)) |

JSON on **`GET /ready`**: `join_phase`, `committed_learner`, `log_caught_up`, `hosts_wired`. Operators debugging a stuck joiner: **`GET /introspect/join-status`** (same pipeline + `local_workers`). ADR: [cluster-elasticity § B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35).

#### Automated regression (B-35)

| Scenario | Location |
|----------|----------|
| Pipeline phase evaluation | `trembita-assembly/src/join_pipeline.rs` (`b35_join_pipeline_scenarios_table`) |
| HTTP 200 iff `pool_ready` | `trembita-dashboard/src/views.rs` (`b35_*`) |
| Ops JSON shape | `trembita-http` `ops_routes.rs`, `introspect_routes.rs` (`b35_*`) |
| Product gateway exposes routes | `trembita/tests/ingress_lb_ops.rs` (`b35_*`) |

```bash
./scripts/test-fast.sh -p trembita-assembly --lib b35_join_pipeline_scenarios_table
./scripts/test-fast.sh -p trembita-dashboard --lib b35_
./scripts/test-fast.sh -p trembita-http --lib b35_
./scripts/test-fast.sh -p trembita --test ingress_lb_ops b35_
```

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

### Elastic join + HTTP LB proof (B-34)

End-to-end story for **«add VPS + same binary + join seeds»** behind an HTTP reverse proxy:

| Proof | Fast (in-process) | Docker (`e2e/elastic_lb.sh`) |
|-------|-------------------|------------------------------|
| Four cluster members each expose **`GET /ready`** with distinct `node_id` | Round-robin over four gateways sees four ids | nginx on host **`:18180`** → ≥3 distinct `node_id` on `/ready` |
| **4th dynamic joiner** after seed + two learners | Simulated four `TrembitaApp` processes | `node4` profile after `node1..3` |
| **Cluster session cookie** — login on A, `GET /me` on B | `b34_cluster_session_cookie_valid_on_peer_gateway` | `DIRECT[1]` login → `DIRECT[2]` `/me` |
| **Secret mismatch** rejects peer cookie | `b34_cluster_session_rejects_peer_cookie_when_secret_differs` | — (covered in-process; set `TREMBITA_GATEWAY_SESSION_SECRET` identically in prod) |
| **PerNode** cap hosts on every node | Directory pool size 4 + `scale_plan` `PerNode` | `/e2e/whoami` via LB hits ≥2 distinct handler `node_id`s |
| Product E2E binary | `trembita-tools/e2e_elastic/cap.rs` (`b34_*`) | Binary `trembita-e2e-elastic` in `Dockerfile.elastic` |

Shared secret in lab: `TREMBITA_GATEWAY_SESSION_SECRET=e2e-elastic-secret-16b` (compose + tests). Session mechanics: [gateway-cluster-auth § B-29/B-40](../decisions/gateway-cluster-auth.md) · [capabilities § B-40](../scenarios/capabilities.md#gateway-auth-split-b-40) · rotation [runbook § B-40](../ops/production-runbook.md#gateway-session-rotation-b-40). Join gating before pool: [cluster-elasticity § B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35). Local **3-node** local 3-node path (no 4th joiner): [local-3node](../../dev/local-3node/README.md) (B-39).

Docker layout: [e2e/docker-compose-elastic.yml](../../e2e/docker-compose-elastic.yml) — direct ops HTTP **`:18181`–`:18184`**, LB **`:18180`**.

#### Automated regression (B-34)

| Scenario | Test / script |
|----------|----------------|
| LB pool sees four `/ready` backends | `b34_lb_pool_distinct_ready_on_four_nodes` |
| Each backend `/ready` reports unique `node_id` | `b34_each_backend_ready_node_id_is_unique` |
| Cookie valid on peer gateway | `b34_cluster_session_cookie_valid_on_peer_gateway` |
| Wrong secret → 401 on peer | `b34_cluster_session_rejects_peer_cookie_when_secret_differs` |
| PerNode directory + boot `scale_plan` | `b34_per_node_cap_directory_spans_four_hosts` |
| E2E cap manifest + JSON shape | `trembita-tools/.../e2e_elastic/cap.rs` (`b34_*`) |
| QUIC join + nginx + session + cap | `./e2e/elastic_lb.sh` |

```bash
./scripts/test-fast.sh -p trembita --test elastic_lb_product b34_
./scripts/test-fast.sh -p trembita-tools --lib b34_
./e2e/elastic_lb.sh   # heavy — CI: MR label run-heavy ([process.md](../process.md))
```

Primary tests: [`elastic_lb_product.rs`](../../crates/trembita/tests/elastic_lb_product.rs). Scenario index: [capabilities § B-34](../scenarios/capabilities.md#elastic-join--lb-b-34).

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
