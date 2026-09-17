# Local 3-node cluster (B-39)

Three **identical** showcase processes on `127.0.0.1`, with the same
**`TREMBITA_GATEWAY_SESSION_SECRET`** on every node so cluster session cookies work
behind a load balancer ([gateway-cluster-auth](../../docs/decisions/gateway-cluster-auth.md),
[ingress-lb](../../docs/ops/ingress-lb.md)).

Default showcase: **`realtime`** (ports **8290–8292**) — `POST /login` → cookie → `GET /me`
on another node.

## Quick start (script)

```bash
./scripts/local-cluster.sh setup
./scripts/local-cluster.sh up
./scripts/local-cluster.sh session-smoke    # login node1 → /me node2
./scripts/local-cluster.sh lb-up            # Docker nginx on :18290
./scripts/local-cluster.sh lb-smoke
./scripts/local-cluster.sh session-smoke --lb
./scripts/local-cluster.sh stop
```

## Debug CLI (same flow)

```bash
cargo build -p trembita-cli
./target/debug/trembita dev cluster-up --setup
./target/debug/trembita dev cluster-lb-up
./target/debug/trembita dev cluster-lb-down
```

Other showcases (`TREMBITA_LOCAL_CLUSTER_SHOWCASE` or `--showcase`):

| Showcase | Ports | Session smoke |
|----------|-------|---------------|
| `realtime` (default) | 8290–8292 | yes |
| `background-jobs` | 8090–8092 | `/ready` only |
| `stateful-workers` | 8190–8192 | `/ready` only |
| `workflows` | 8490–8492 | `/ready` only |

Use **`GET /ready`** for LB backends ([ingress-lb § B-35](../../docs/ops/ingress-lb.md#join-readiness-pipeline-b-35)). Heavier CI elastic proof (dedicated E2E binary): [e2e/elastic_lb.sh](../../e2e/elastic_lb.sh) ([capabilities § B-34](../../docs/scenarios/capabilities.md#elastic-join--lb-b-34)).

## Local elastic parity (B-42)

Four **realtime** nodes on **8290–8293** with the same staged join story as B-34 — without Docker trembita containers:

```bash
./scripts/local-cluster.sh setup
./scripts/local-cluster.sh elastic-up      # or: up --nodes 4
./scripts/local-cluster.sh lb-up           # nginx includes 4th upstream when node4 /ready
./scripts/local-cluster.sh elastic-smoke   # /ready spread + session + GET /e2e/whoami
./scripts/local-cluster.sh stop
```

Debug CLI: `./target/debug/trembita dev cluster-up --setup --nodes 4 --lb` · `./target/debug/trembita dev cluster-lb-up --nodes 4`

| Phase | What it checks (realtime) |
|-------|---------------------------|
| `elastic-up` | Staged join: nodes 1–3 → `/ready` → node 4 |
| `lb-smoke` | ≥2 distinct `node_id` (3 nodes) or ≥3 (4 nodes) on `/ready` |
| `session-smoke` | Cluster cookie: login node1 → `/me` node2 (or via LB with `--lb`) |
| `cap-smoke` | PerNode **`GET /e2e/whoami`** via LB — ≥2 handler `node_id`s |
| `elastic-smoke` | All of the above in one pass |

**Authoritative regression index:** [capabilities § B-42](../../docs/scenarios/capabilities.md#automated-regression-b-42) (full `b42_*` table + smoke thresholds).

### Automated regression (B-39)

| Scenario | Regression |
|----------|------------|
| Secret ≥16 chars, default showcase `realtime` @ 8290 | `b39_local_cluster_gateway_secret_meets_env_min_length`, `b39_cluster_up_defaults_to_realtime` |
| Invalid node count (0, 9) | `b39_cluster_up_rejects_invalid_node_count` |
| `dev/local-3node/*` + `local-cluster.sh` exist | `b39_local_3node_packaging_files_exist_in_repo` |
| Constants match script env defaults | `b39_local_cluster_secret_and_token_match_script_defaults` |
| `render_lb_nginx_ports` for multiple base ports | `b39_lb_nginx_render_scenarios_table` |
| Realtime `listen_addr` + `join_seed` | `b39_realtime_showcase_three_node_addrs_and_join_seed` |
| Repo nginx template → docker backends | `b39_repo_nginx_template_renders_realtime_backends` |
| Showcase `base_port` table vs script | `b39_local_cluster_showcase_base_ports_match_script_table` |

### Automated regression (B-42)

| Scenario | Regression |
|----------|------------|
| Staged join + spawn waves | `b42_use_staged_elastic_join_scenarios_table`, `b42_cluster_spawn_waves_scenarios_table` |
| Smoke thresholds (LB 2/3, cap 2) | `b42_lb_smoke_min_distinct_scenarios_table`, `b42_cap_smoke_min_distinct_matches_e2e_elastic` |
| nginx / `cluster-lb-up` backends | `b42_clamp_lb_backends_scenarios_table`, `b42_resolve_lb_backend_count_scenarios_table` |
| Join seed + cert id 4 | `b42_join_seed_points_at_node_one_for_all_cluster_showcases`, `b42_cluster_showcases_cert_id_four_present` |
| Script + E2E `/e2e/whoami` | `b42_local_cluster_script_elastic_phases_exist`, `b42_elastic_lb_e2e_script_uses_same_whoami_path_as_local_cap_smoke` |

```bash
./scripts/test-fast.sh -p trembita-cli --lib b39_
./scripts/test-fast.sh -p trembita-cli --lib b42_
./scripts/test-fast.sh -p trembita-cli --test dev b42_
./scripts/test-fast.sh -p trembita-tools --lib b42_
```

Scenario index: [capabilities § B-39](../../docs/scenarios/capabilities.md#local-3-node-cluster-b-39) · [B-42](../../docs/scenarios/capabilities.md#local-elastic-parity-b-42).

## Env (set automatically by `cluster-up`)

| Variable | Local dev value |
|----------|-----------------|
| `TREMBITA_GATEWAY_SESSION_SECRET` | `trembita-local-3node-dev-secret` |
| `GATEWAY_TOKEN` | `dev-secret` (realtime login) |

Copy the same secret into **`deploy/.env`** for multi-node product apps
([env.md](../../docs/env.md)).

## LB curl (manual)

Direct backends:

```bash
curl -sf http://127.0.0.1:8290/ready | jq .
curl -sf http://127.0.0.1:8291/ready | jq .
```

Through nginx (after `lb-up`):

```bash
curl -sf http://127.0.0.1:18290/ready
```

## Files

| Path | Purpose |
|------|---------|
| [`scripts/local-cluster.sh`](../../scripts/local-cluster.sh) | Phase 1 entrypoint |
| [`nginx.conf.template`](nginx.conf.template) | Rendered to `nginx.generated.conf` for Docker |
| [`docker-compose.yml`](docker-compose.yml) | nginx only — not a trembita orchestrator |

## See also

- [`dev/3node/README.md`](../3node/README.md) — raw `trembita-node` (no product gateway)
- [`e2e/elastic_lb.sh`](../../e2e/elastic_lb.sh) — CI heavy lane (4 nodes + LB)
