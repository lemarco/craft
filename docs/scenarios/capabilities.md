# Capabilities — typed ops on the cluster

**Status:** Shipped (B-21) — [capability-dx ADR](../decisions/capability-dx.md) (Accepted).

## When to use

- You want **one handler** callable **inline** (await reply) or **queued** (retries, DLQ) without duplicating logic
- Stateful work pinned by key (`OrderId`, tenant, …) without writing mailbox boilerplate
- Product code stays in **`capabilities/` + `domain/`**; not runtime worker traits

**Prefer** [`CapManifest`](../../crates/trembita/src/capability/manifest.rs) for product ops. Legacy [`WorkerOpts`](../../crates/trembita/src/worker_opts.rs) / `UserActor` in app code remains for migration demos and advanced runtime use.

## Quick sketch

```rust
// capabilities/orders.rs — wire op `process_order` from struct name `ProcessOrder`
#[cap_handler(group = "orders", key = "order_id")]
async fn process_order(msg: ProcessOrder, state: &mut OrdersState) -> Result<Ack, CapError> { … }

// manifest.rs — registration
CapManifest::new().group(cap_register_chain!(
    CapGroup::<OrdersState>::for_cap::<ProcessOrder>()
        .default_queue_for::<ProcessOrder>()
        .default_event_ingress_for::<ProcessOrder>(),
    process_order_register,
));

// call site
ProcessOrder { id }.via(&app).route(Route::Inline).await?;
ProcessOrder { id }.via(&app).enqueue().await?; // dedup from `cap_key()` when `key = "…"` on handler
ProcessOrder { id }.via(&app).dedup_key("client-token").enqueue().await?;
ProcessOrder { id }.via(&app).queued_wait().await?;
ProcessOrder { id }.via(&app).publish_event().await?; // Route::Event egress
```

## HTTP (gateway-first)

Declare paths in `http/product.rs` — not on the manifest:

```rust
ProductRoutes::new()
    .post_identity("/orders/submit", cap_fire::<ProcessOrder>(state))
    .post("/orders/query", cap_invoke::<ProcessOrder>(state, Route::Inline))
    .build()
```

Greenfield apps do **not** rely on `/actors/.../cast` — see [capability-greenfield-wire](../decisions/capability-greenfield-wire.md).

## Routes (semantics)

| Route | Use |
|-------|-----|
| `Inline` | Short RPC-style await |
| `InlineFire` | Cast, no reply — use `.fire()` |
| `Queued` | Backlog + at-least-once — use `.enqueue()` |
| `QueuedWait` | Enqueue + await stored reply — use `.queued_wait()` |
| `Scheduled` | Delayed enqueue — `.run_at_ms(ms).schedule()` |
| `Session` | Sticky session — `.session_key(k).route(Route::Session)` |
| `Event` | Topic — `.event_ingress(topic, sub)` + `.publish_event()`; subscriber runs same handler |

Authoritative domain data still lives in Raft SM or external DB — see [state-placement](state-placement.md). Platform limits (R1–R4, query vs ask, sagas): [structural-limits](structural-limits.md).

## Group scale (B-28)

Omit [`.instances(n)`](../../crates/trembita/src/capability/group.rs) unless you need an explicit fixed pool:

| Your group | Default hosts |
|------------|----------------|
| Marker-only `State` (zero-sized), inline/queued ops | **One per live node** — add VPS → more cap handlers without manifest changes |
| Non–zero-sized `State` (shared RAM in the host) | **One cluster-wide** — use `.instances(n)` or `.per_node()` to override |
| Any op with `Route::Session` | **One cluster-wide** — realtime apps usually add `.per_node()` ([realtime](../../examples/realtime/)) |

ADR: [capability-dx § Group scale (B-28)](../decisions/capability-dx.md#group-scale-b-28).

### Automated regression (B-28)

| Area | Location |
|------|----------|
| `resolved_scale` defaults (marker / RAM / session) | `trembita/src/capability/group.rs` (`#[cfg(test)]`, overlaps B-31 runtime defaults table) |
| 3-node PerNode pool + shared-RAM single host + session Fixed(1) | `trembita/src/integration/cap_scale.rs` |

```bash
./scripts/test-fast.sh -p trembita --lib product_scale_b31_runtime_defaults_table
./scripts/test-fast.sh -p trembita --lib auto_scale_marker_state_spawns_one_host_per_node
./scripts/test-fast.sh -p trembita --lib auto_scale_shared_ram_state_spawns_single_cluster_host
./scripts/test-fast.sh -p trembita --lib auto_scale_session_route_defaults_to_single_host
```

## Product scale (B-31)

When you add VPS nodes with the same binary, scale follows **how the op is invoked**, not a single “autoscale” knob:

| You call | Cluster behavior |
|----------|------------------|
| `Route::Inline` / `InlineFire` on a marker-only group | More nodes → more cap hosts (default **PerNode**) |
| `#[cap_handler(key = "…")]` | One logical owner per key (shard / sticky compute) |
| `Route::Session` | Sticky to session host; pools usually **`.per_node()`** |
| `Route::Queued` | Consumers drain the stream cluster-wide; **handlers** follow group scale |

**Foot-gun:** `.instances(1)` + queued wiring without keyed handlers — queue depth grows on all nodes but only one host runs handlers. **`trembita doctor`** reports an `[error]`; remove `.instances(1)` or add keys / shared-state intent.

Full table: [capability-dx § Product scale model](../decisions/capability-dx.md#product-scale-model-b-31).

### Automated regression (B-31)

| Area | Location |
|------|----------|
| Doctor foot-guns (table + scaffold) | `trembita-cli/src/scaffold/doctor.rs` (`product_scale_b31_*`) |
| Doctor on real scaffold tree | `trembita-cli/tests/cap_scale_doctor.rs` |
| Runtime scale defaults vs product scale map | `trembita/src/capability/group.rs` (`product_scale_b31_runtime_defaults_table`) |
| 3-node host placement | `trembita/src/integration/cap_scale.rs` (B-28) |

```bash
./scripts/test-fast.sh -p trembita-cli --lib product_scale_b31
./scripts/test-fast.sh -p trembita-cli --test cap_scale_doctor
./scripts/test-fast.sh -p trembita --lib product_scale_b31
```

## Scale & scaffold DX (B-38)

CLI improvements on top of [B-31 product scale](#product-scale-b-31) — faster onboarding, actionable doctor output, profile-based scaffolds.

| Tool | Purpose |
|------|---------|
| `trembita doctor --explain-scale` | **Product scale narrative** + B-31 foot-gun checks only — **no** layout / missing-path lint ([`run_explain_scale`](../../crates/trembita-cli/src/scaffold/doctor.rs)) |
| `trembita doctor` (lint v2) | [`DoctorFinding::suggestion`](../../crates/trembita-cli/src/scaffold/doctor.rs) — optional `→` hint on scale foot-guns and R4 store misuse |
| `trembita new --profile …` | Alias for `--template` via [`AppTemplate::parse_profile`](../../crates/trembita-cli/src/scaffold/template.rs) |
| Jobs template **`capabilities/task.rs`** | Sample **queued** cap with `require_store()` + idempotency marker ([R4](structural-limits.md#r4--handler-ram-without-cap-store)) |

**Profiles** (same as `--template`; aliases in parentheses):

| Profile | Layout highlights |
|---------|-------------------|
| `jobs` (`job`) | `task.rs` queued idempotency, `consumers/sample.rs`, `cap_enqueue::<RunTask>` in `http/product.rs` |
| `realtime` (`real-time`, `ws`, `websocket`) | Session chat cap with **`.per_node()`** |
| `api` (`http`, `gateway`) | Inline **`ping`** cap, **no** default job consumer |
| `workflows`, `topics` | Unchanged template ids (round-trip via `b38_template_id_roundtrip_table`) |

```bash
trembita new my-app --profile jobs
trembita doctor --explain-scale    # before changing scale in manifest
trembita doctor --preflight        # deploy checklist (B-33 join + session still applies)
```

Runbook: [production-runbook § B-38](../ops/production-runbook.md#scale-scaffold-dx-b-38) · ADR: [capability-dx § B-38](../decisions/capability-dx.md#scale-scaffold-dx-b-38) · CLI: [framework-conventions § CLI](../decisions/framework-conventions.md#cli).

### Automated regression (B-38)

| Scenario | Regression |
|----------|------------|
| `parse_profile` ≡ `parse_name` for alias table | `b38_parse_profile_matches_template_aliases_table` |
| Every template `id()` round-trips | `b38_template_id_roundtrip_table` |
| `api` profile feature set (gateway, no jobs) | `b38_api_profile_omits_jobs_feature` |
| Scale foot-gun suggestions (queued / stateless / session) | `b38_scale_footgun_suggestions_scenarios_table` |
| R4 `store_get` without `require_store` → points at `task.rs` | `b38_store_guard_suggestions_scenarios_table` |
| `--explain-scale` skips layout errors | `b38_explain_scale_skips_layout_lint` |
| Jobs scaffold includes `task.rs` + store wiring | `b38_template_jobs_includes_queued_idempotency_capability` |
| `--profile jobs` same tree as `--template jobs` | `b38_profile_jobs_matches_template_jobs_layout` |
| Realtime scaffold `.per_node()` + session | `b38_profile_realtime_includes_session_per_node_wiring` |
| API scaffold no `consumers/sample.rs` | `b38_profile_api_omits_job_consumer` |
| Doctor error on bad queue + suggestion text | `b38_queued_footgun_error_includes_fix_suggestion` |
| Jobs template passes R4 doctor | `b38_jobs_template_passes_r4_store_doctor` |
| Explain-scale on realtime / jobs scaffolds | `b38_explain_scale_on_realtime_scaffold_mentions_session`, `b38_explain_scale_on_jobs_scaffold_lists_queued_guidance` |

```bash
./scripts/test-fast.sh -p trembita-cli --lib b38_
./scripts/test-fast.sh -p trembita-cli --test scaffold b38_
./scripts/test-fast.sh -p trembita-cli --test cap_scale_doctor b38_
```

## Local 3-node cluster (B-39)

**Dev-only** packaging to run **three (or four, [B-42](#local-elastic-parity-b-42)) identical showcase binaries** on `127.0.0.1` with the same **`TREMBITA_GATEWAY_SESSION_SECRET`** on every node — exercise cluster session cookies and optional nginx round-robin before the heavier [B-34](#elastic-join--lb-b-34) Docker E2E binary.

| Entry | Purpose |
|-------|---------|
| [`scripts/local-cluster.sh`](../../scripts/local-cluster.sh) | `setup` / `up` / `session-smoke` / `lb-up` / `lb-smoke` / `stop` ([B-42](#local-elastic-parity-b-42): `elastic-up`, `elastic-smoke`, `cap-smoke`) |
| `trembita dev cluster-up` | Same flow via debug CLI ([`local_cluster.rs`](../../crates/trembita-cli/src/dev/local_cluster.rs)); `--nodes 4` = staged elastic join |
| `trembita dev cluster-lb-up` | Docker nginx on **`:18290`** (3 or 4 upstreams) — [`dev/local-3node/`](../../dev/local-3node/README.md) |

Default showcase **`realtime`** (ports **8290–8292**): `POST /login` on node1 → `GET /me` on node2 with cookie. **`background-jobs`** uses the same 3-node layout (**8090–8092**) but LB smoke is **`/ready`** only (no session routes).

| Showcase | Base port (node1–3) | Session smoke |
|----------|---------------------|---------------|
| `realtime` | 8290–8292 | yes (`session-smoke`, `--lb`) |
| `background-jobs` | 8090–8092 | `/ready` only |
| `stateful-workers` | 8190–8192 | `/ready` only |
| `workflows` | 8490–8492 | `/ready` only |

Local dev env (set by `cluster-up`): `TREMBITA_GATEWAY_SESSION_SECRET=trembita-local-3node-dev-secret`, `GATEWAY_TOKEN=dev-secret`. LB health checks should use **`GET /ready`** ([B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35)).

**Not production orchestration** — Docker Compose here is **nginx demo only** ([deployment-model](../decisions/deployment-model.md)). Production elastic proof: [Elastic join + LB (B-34)](#elastic-join--lb-b-34).

Guide: [dev/local-3node/README.md](../../dev/local-3node/README.md) · ADR: [capability-dx § B-39](../decisions/capability-dx.md#local-3-node-cluster-b-39).

### Automated regression (B-39)

| Scenario | Regression |
|----------|------------|
| Secret length, default showcase, LB port constants | `b39_local_cluster_gateway_secret_meets_env_min_length`, `b39_cluster_up_defaults_to_realtime` |
| `cluster-up` rejects 0 or 9 nodes | `b39_cluster_up_rejects_invalid_node_count` |
| Repo packaging (`docker-compose`, nginx template, script) | `b39_local_cluster_3node_packaging_files_exist_in_repo` |
| Rust constants match script defaults | `b39_local_cluster_secret_and_token_match_script_defaults` |
| nginx port substitution (8290 / 8090 / 8190 bases) | `b39_lb_nginx_render_scenarios_table` |
| Realtime listen addrs + join seed | `b39_realtime_showcase_three_node_addrs_and_join_seed` |
| Template renders `host.docker.internal:829x` | `b39_repo_nginx_template_renders_realtime_backends` |
| Showcase base ports match `local-cluster.sh` | `b39_local_cluster_showcase_base_ports_match_script_table` |

```bash
./scripts/test-fast.sh -p trembita-cli --lib b39_
./scripts/test-fast.sh -p trembita-cli --test dev b39_
```

## Local elastic parity (B-42)

Same mental model as [Elastic join + LB (B-34)](#elastic-join--lb-b-34) on a laptop: **staged 4th joiner**, nginx round-robin over **3 or 4** backends, cluster session smoke, **`GET /e2e/whoami`** PerNode cap spread via LB. Uses the **realtime** showcase (registers shared [`e2e_pool`](../../crates/trembita-tools/src/e2e_elastic/cap.rs)) — not the dedicated elastic E2E binary.

| Entry | Purpose |
|-------|---------|
| `./scripts/local-cluster.sh elastic-up` | `--nodes 4` via debug CLI (seed 1–3 → wait `/ready` → joiner 4) |
| `./scripts/local-cluster.sh elastic-smoke` | `lb-smoke` (≥3 distinct `node_id` when 4 nodes) + session + `cap-smoke` |
| `./scripts/local-cluster.sh cap-smoke` | PerNode inline cap via LB (realtime only) |
| `trembita dev cluster-up --nodes 4 [--lb]` | Same staging + optional nginx **:18290** with 4th upstream |

Ports (**realtime**): **8290–8293**. Guide: [dev/local-3node § B-42](../../dev/local-3node/README.md#local-elastic-parity-b-42).

**Smoke thresholds** (same as [`e2e/elastic_lb.sh`](../../e2e/elastic_lb.sh)):

| Check | 3 nodes | 4 nodes |
|-------|---------|---------|
| `lb-smoke` distinct `node_id` on `/ready` | ≥ **2** | ≥ **3** |
| `cap-smoke` distinct handler `node_id` on `/e2e/whoami` | ≥ **2** (LB required) | ≥ **2** |

Rust mirrors: [`lb_smoke_min_distinct`](../../crates/trembita-cli/src/dev/local_cluster.rs), [`cap_smoke_min_distinct`](../../crates/trembita-cli/src/dev/local_cluster.rs), [`resolve_lb_backend_count`](../../crates/trembita-cli/src/dev/local_cluster.rs) (CLI `cluster-lb-up --nodes`).

### Automated regression (B-42)

| Scenario | Regression |
|----------|------------|
| Elastic default node count = 4 | `b42_elastic_default_four_nodes_constant`, `b42_elastic_node_count_default_is_four` |
| Fourth nginx upstream when `backends=4` | `b42_repo_nginx_template_renders_fourth_backend_when_elastic`, `b42_workflows_fourth_nginx_upstream_port`, `b42_render_lb_clamp_drops_fourth_upstream_for_low_backend_count` (placeholder `${LOCAL_CLUSTER_NODE4_SERVER}` in `b39_local_3node_packaging_files_exist_in_repo`) |
| LB upstream count clamped to 3..=4 | `b42_clamp_lb_backends_scenarios_table` |
| Fourth listen addr per showcase (8093 / 8193 / 8293 / 8493) | `b42_showcase_fourth_listen_addr_scenarios_table`, `b42_realtime_fourth_listen_addr_matches_elastic_ports` |
| Staged join policy + spawn waves (`nodes=0` … `5`) + `/ready` ports | `b42_use_staged_elastic_join_scenarios_table`, `b42_cluster_spawn_waves_scenarios_table`, `b42_ready_port_for_node_scenarios_table`, `b42_ready_port_matches_showcase_listen_addr` |
| `--nodes 4` valid before binary/setup errors | `b42_cluster_up_four_nodes_rejects_invalid_nodes_not_first` |
| LB/cap distinct thresholds vs E2E script | `b42_lb_smoke_distinct_thresholds_match_e2e_elastic_script`, `b42_lb_smoke_min_distinct_scenarios_table`, `b42_cap_smoke_min_distinct_matches_e2e_elastic`, `b42_e2e_and_local_cap_smoke_min_distinct_documented` |
| `cluster-lb-up` backend count (explicit vs probe) | `b42_resolve_lb_backend_count_scenarios_table` |
| Join seed + cert id 4 for elastic joiners | `b42_join_seed_points_at_node_one_for_all_cluster_showcases`, `b42_cluster_showcases_cert_id_four_present`, `b42_cluster_showcases_include_node_four_cert_id` |
| Script / CLI packaging | `b42_script_elastic_up_forces_four_nodes`, `b42_cli_main_cluster_up_mentions_elastic_nodes_four` |
| Realtime `e2e_pool` wiring | `b42_realtime_example_wires_e2e_pool_manifest` |
| Script phases mirror `e2e/elastic_lb.sh` | `b42_local_cluster_script_elastic_phases_exist`, `b42_elastic_lb_e2e_script_uses_same_whoami_path_as_local_cap_smoke` |
| Shared **`/e2e/whoami`** path + cap metadata | `b42_whoami_path_matches_elastic_lb_and_local_cluster_scripts`, `b42_whoami_cap_request_metadata_scenarios_table`, `b42_node_reply_json_wire_fields_scenarios_table` |
| Staged join waits on `/ready` (nodes ≥ 4) | [`cluster.rs`](../../crates/trembita-cli/src/dev/cluster.rs) — live check: `./scripts/local-cluster.sh elastic-smoke` |

```bash
./scripts/test-fast.sh -p trembita-cli --lib b39_
./scripts/test-fast.sh -p trembita-cli --lib b42_
./scripts/test-fast.sh -p trembita-cli --test dev b42_
./scripts/test-fast.sh -p trembita-tools --lib b42_
./scripts/test-fast.sh -p trembita-tools --lib b34_
```

## Gateway auth split (B-40)

Cluster cookie login ([B-29](../decisions/gateway-cluster-auth.md)) is implemented through **`SessionIssuer`** / **`SessionVerifier`** ports — HTTP [`SessionGate`](../../crates/trembita-http/src/routing/session_ports.rs) validates cookies via a verifier; login / OIDC callbacks call an issuer and [`set_session_cookie`](../../crates/trembita-http/src/routing/auth.rs). **No session storage inside `trembita-gateway-auth`** — only flow + `SessionIssuer`.

| Pattern | When |
|---------|------|
| Signed cookie (stateless) | Default — `TREMBITA_GATEWAY_SESSION_SECRET` on every node ([B-34](#elastic-join--lb-b-34), [B-39](#local-3-node-cluster-b-39)) |
| Cap-store opaque token | Logout/revoke registry when cap store is cluster-visible |
| `trembita-gateway-auth` + `gateway-auth` feature | Dev OIDC-shaped callback; production IdP replaces [`DevOidcCallback`](../../crates/trembita-gateway-auth/src/dev_oidc.rs) |

**Rotation:** `TREMBITA_GATEWAY_SESSION_SECRET_PREVIOUS` during secret roll — [env.md](../env.md) · [runbook § B-40](../ops/production-runbook.md#gateway-session-rotation-b-40).

Demo: [`examples/oauth-gateway`](../../examples/oauth-gateway/README.md).

### Automated regression (B-40)

| Scenario | Regression |
|----------|------------|
| Verifier-driven gate (table) | `b40_session_gate_from_verifier_scenarios_table` (`trembita-http`) |
| Signed + cap-store port roundtrips, rotation, multi-cookie | `b40_*` in `gateway/cluster_session.rs` |
| Product HTTP login + peer `/me` + revoke | `b40_signed_cookie_issuer_login_then_verifier_me_route`, `b40_capstore_revoked_session_returns_unauthorized_on_me`, `b40_rotating_gate_accepts_token_signed_with_previous_secret` |
| Issue session + dev OIDC | `b40_issue_gateway_session_json_body_and_set_cookie`, `b40_dev_oidc_callback_*` (`trembita-gateway-auth`) |

```bash
./scripts/test-fast.sh -p trembita-http --lib b40_
./scripts/test-fast.sh -p trembita --lib b40_
./scripts/test-fast.sh -p trembita --test gateway_cluster_session b40_
./scripts/test-fast.sh -p trembita-gateway-auth --lib b40_
```

ADR: [gateway-cluster-auth § B-40](../decisions/gateway-cluster-auth.md#b-40--logic--storage-split).

## Coordination scale (B-32)

When job enqueue or keyed coordination (cap store, topics, multi-group Raft) outgrow one Meta-Raft group, wire scale through the **product** surface — [`QueueOpts` / `JobOpts`](../../crates/trembita/src/queue_opts.rs), [`TrembitaConfigure`](../../crates/trembita/src/configure.rs), and `TREMBITA_JOB_QUEUE_*` / `TREMBITA_RAFT_*` env — not only assembly `TrembitaClusterBuilder`.

| Need | Product | Env |
|------|---------|-----|
| Fixed queue shards | `.sharded(n)` on queue / job opts | `TREMBITA_JOB_QUEUE` + `TREMBITA_JOB_QUEUE_SHARDS` |
| Adaptive queue shards | `.auto_shard()` | `TREMBITA_JOB_QUEUE_AUTO_SHARD=1` |
| Multi-Raft coordination | `.with_coordination_raft_groups(n)` | `TREMBITA_RAFT_GROUPS`, optional `TREMBITA_RAFT_SHARD_COUNT` |
| Expand catalog live | [`TrembitaApp::add_raft_groups`](../../crates/trembita/src/app/runtime.rs) | — |

Full map: [capability-dx § Coordination scale](../decisions/capability-dx.md#coordination-scale-b-32).

## Coordination growth presets (B-37)

Bundled profiles for [B-32 coordination scale](#coordination-scale-b-32) — [`CoordinationGrowthPreset`](../../crates/trembita/src/configure.rs) in code or **`TREMBITA_COORDINATION_PROFILE`** in deploy env. Resolution lives in [`coordination_profile.rs`](../../crates/trembita-assembly/src/coordination_profile.rs); manifest wiring applies the preset to **standard** [`QueueOpts`](../../crates/trembita/src/queue_opts.rs) / [`JobOpts`](../../crates/trembita/src/job_opts.rs) only (already `.sharded(n)` queues ignore the preset).

| Preset (`env_key`) | Coordination Raft | Queue auto-shard (standard streams) | Leader [`AutoShardPolicy`](../../crates/trembita-jobs/src/queue_auto_shard.rs) |
|--------|-------------------|--------------------------------------|---------------|
| `standard` | 1 group | off | default (unused) |
| `jobs_backlog` | 1 group | on | pending ≥ **256**, **2** hot ticks, max **16** shards |
| `write_sharding` | **2** groups, **64** virtual shards | off | — |
| `full` | **2** groups, **64** shards | on | pending ≥ **512**, **3** hot ticks, max **12** shards |

**Env aliases** (parse is case-insensitive): `jobs-backlog` / `jobs` → `jobs_backlog`; `write-sharding` / `sharding` / `write` → `write_sharding`; `growth` → `full`; empty → `standard`.

**Overrides:** explicit `TREMBITA_RAFT_GROUPS`, `TREMBITA_RAFT_SHARD_COUNT`, and `TREMBITA_JOB_QUEUE_AUTO_SHARD` win over the profile. In code, [`.with_coordination_raft_groups`](../../crates/trembita/src/configure.rs) after the preset replaces group count but keeps preset shard defaults when unset (`b37_explicit_raft_groups_after_preset_override_profile`).

```rust
TrembitaApp::builder()
    .configure(
        TrembitaConfigure::default()
            .with_data_dir("/var/lib/app")
            .with_coordination_growth_preset(CoordinationGrowthPreset::Full),
    )
    .manifest(AppManifest::new().queue([QueueOpts::new("jobs", lease)]))
```

**Verify after deploy:** [`GET /introspect/product-scale`](../../crates/trembita/src/app/scale_plan.rs) or boot log `trembita::product_scale` — expect `coordination_raft_groups`, queue `mode` (`standard` vs `auto_shard`). Runbook: [production-runbook § B-37](../ops/production-runbook.md#coordination-growth-preset-b-37).

When to enable: [getting-started § B-37](../getting-started.md#when-to-enable-coordination-growth-b-37).

### Automated regression (B-37)

| Scenario | Regression |
|----------|------------|
| Parse canonical + alias strings | `b37_profile_parse_table`, `b37_profile_parse_aliases_table` |
| `env_key()` round-trip | `b37_profile_env_key_matches_parse_roundtrip` |
| All four presets match spec table | `b37_profile_spec_scenarios_table` |
| `WriteSharding` / `JobsBacklog` spec smoke | `b37_write_sharding_spec_sets_multi_raft`, `b37_jobs_backlog_auto_shard_policy_has_ceiling` |
| Env-only auto-shard flag per preset | `b37_env_job_queue_auto_shard_from_profile_spec_table` |
| Write-sharding fills Raft when env unset | `b37_profile_write_sharding_fills_raft_when_env_unset` |
| `TrembitaConfigure::with_coordination_growth_preset` | `b37_configure_growth_preset_scenarios_table` |
| Explicit Raft groups override preset count | `b37_explicit_raft_groups_after_preset_override_profile` |
| Preset on standard vs sharded queue opts | `b37_queue_growth_preset_scenarios_table` |
| Same for job stream opts | `b37_job_opts_growth_preset_scenarios_table` |
| Leader depth / ceiling for `jobs_backlog` + `full` | `b37_growth_preset_auto_shard_policies_table` |
| Boot: `standard` → 1 group, standard queue | `b37_standard_preset_keeps_single_raft_standard_queue` |
| Boot: `jobs_backlog` → auto_shard queue | `b37_jobs_backlog_preset_applies_auto_shard_to_standard_queue` |
| Boot: `write_sharding` → 2×64 Raft, standard queue | `b37_write_sharding_preset_boots_multi_raft_without_auto_shard_queue` |
| Boot: `full` → 2×64 + auto_shard | `b37_full_preset_boots_multi_raft_with_auto_shard_queue` |

```bash
./scripts/test-fast.sh -p trembita-assembly --lib b37_
./scripts/test-fast.sh -p trembita-jobs --lib b37_
./scripts/test-fast.sh -p trembita --lib b37_
./scripts/test-fast.sh -p trembita --test coordination_growth_preset b37_
```

ADR: [capability-dx § B-37](../decisions/capability-dx.md#coordination-growth-presets-b-37) · [env.md § `TREMBITA_COORDINATION_PROFILE`](../env.md).

### Automated regression (B-32)

| Area | Location |
|------|----------|
| Configure + queue/job scale tables | `trembita/src/{configure,queue_opts,job_opts}.rs` (`b32_*_scenarios_table`) |
| Env parse (shards vs auto-shard, Raft groups) | `trembita-assembly/src/env_config.rs` (`b32_*_scenarios_table`) |
| Product boot (sharded / auto-shard / multi-Raft / `from_config`) | `trembita/tests/product_coordination_scale.rs` |

```bash
./scripts/test-fast.sh -p trembita --lib b32_
./scripts/test-fast.sh -p trembita --test product_coordination_scale --all-features
./scripts/test-fast.sh -p trembita-assembly --lib b32_
```

## Boot scale report + preflight (B-33)

After `wait_until_ready`, the product logs `target: trembita::product_scale` and serves the same snapshot at **`GET /introspect/product-scale`** (capability host labels, queue shard mode, coordination Raft layout). Multi-node clusters also rely on **remote `CapHost` spawn** so PerNode / `Fixed(n>1)` pools are not pinned to the boot node.

| Check | Where |
|-------|--------|
| `ProductScalePlan` labels + HTTP JSON parity | `trembita/src/app/scale_plan.rs` (`b33_*`), `tests/product_scale_report.rs` |
| 3-node directory pool + plan hosts (`PerNode` / `Fixed(2)` / `Fixed(1)`) | `trembita/src/integration/cap_scale.rs` |
| `trembita doctor --preflight` join seeds ↔ session secret | `trembita-cli/src/scaffold/doctor.rs` (`b33_preflight_*`), `tests/cap_scale_doctor.rs` |

```bash
./scripts/test-fast.sh -p trembita --lib b33_
./scripts/test-fast.sh -p trembita --test product_scale_report
./scripts/test-fast.sh -p trembita --lib auto_scale_marker_state_spawns_one_host_per_node
./scripts/test-fast.sh -p trembita --lib explicit_instances_pins_cluster_wide_pool_on_marker_state
./scripts/test-fast.sh -p trembita-cli --lib b33_preflight_join_seeds_scenarios_table
./scripts/test-fast.sh -p trembita-cli --test cap_scale_doctor
```

Runbook: [production-runbook § Product scale introspection](../ops/production-runbook.md#product-scale-introspection-b-33).

## Elastic join + LB (B-34)

Homogeneous growth behind an HTTP LB: **same binary**, **`TREMBITA_JOIN_SEEDS`**, pool checks on **`GET /ready`** (not `/health`), identical **`TREMBITA_GATEWAY_SESSION_SECRET`** on every gateway so a cookie minted on node A validates on node B, and **PerNode** capability groups so inline routes (e.g. `/e2e/whoami`) can hit any member as the cluster grows.

| Scenario | Regression |
|----------|------------|
| Simulated LB round-robin over four gateways → four distinct `node_id` on `/ready` | `b34_lb_pool_distinct_ready_on_four_nodes` |
| Each process exposes its own `node_id` on direct `/ready` | `b34_each_backend_ready_node_id_is_unique` |
| Login on gateway 1 → `GET /me` on gateway 2 | `b34_cluster_session_cookie_valid_on_peer_gateway` |
| Peer rejects cookie when `TREMBITA_GATEWAY_SESSION_SECRET` differs | `b34_cluster_session_rejects_peer_cookie_when_secret_differs` |
| `e2e_pool` directory lists four hosts; `scale_plan` shows `PerNode` | `b34_per_node_cap_directory_spans_four_hosts` |
| Docker E2E binary registers `e2e_pool` + JSON reply | `trembita-tools/src/e2e_elastic/cap.rs` (`b34_*`) |
| Live QUIC: seed + 3 joiners, **4th joiner**, nginx **`:18180`**, session + cap smoke | [`e2e/elastic_lb.sh`](../../e2e/elastic_lb.sh) |

```bash
./scripts/test-fast.sh -p trembita --test elastic_lb_product b34_
./scripts/test-fast.sh -p trembita-tools --lib b34_
./e2e/elastic_lb.sh   # Docker — CI label run-heavy
```

Ops recipe (health vs ready, session cookies, WS affinity): [ingress-lb § B-34](../ops/ingress-lb.md#elastic-join--http-lb-proof-b-34) · [e2e/README § elastic_lb.sh](../../e2e/README.md#elastic-lb-b-34) · [cluster-elasticity § B-34](../decisions/cluster-elasticity.md#elastic-join--http-lb-proof-b-34).

## Join readiness pipeline (B-35)

When a node joins via **`TREMBITA_JOIN_SEEDS`**, it must not receive LB traffic until the **join pipeline** completes: committed membership → Raft log catch-up → (for **learners**) local supervisor **auto-hosts**. Until then **`GET /ready`** is **503** with `join_phase` ≠ `pool_ready`. Seeds and voters reach **`pool_ready`** after catch-up; learners also need **`hosts_wired`** (non-empty local worker/cap host list).

| Scenario | Regression |
|----------|------------|
| Five-row phase table (membership / catch-up / voter ready / learner awaiting hosts / learner pool-ready) | `b35_join_pipeline_scenarios_table` |
| `/ready` returns 200 only for `pool_ready` (incl. draining voter) | `b35_is_ready_join_phase_scenarios_table` |
| `JoinStatusView` mirrors `Readiness` | `b35_join_status_view_from_readiness_preserves_phase_and_workers` |
| `join_phase` serialized as snake_case JSON | `b35_join_phase_serializes_snake_case` |
| Introspect route returns pipeline fields | `b35_join_status_json_reflects_readiness_pipeline` |
| Single-node product boot reaches `pool_ready` on `/ready` | `b35_ready_json_includes_join_phase_after_boot` |
| Default ops table registers `/introspect/join-status` | `b35_product_ops_exposes_join_status_route` |

**Product boot:** env join seeds enable [`ReadyOpts::pool_membership`](../../crates/trembita-assembly/src/ready.rs) — apps wait for LB-safe readiness, not leadership ([`run_hint`](../../crates/trembita/src/app/run_hint.rs)).

```bash
./scripts/test-fast.sh -p trembita-assembly --lib b35_join_pipeline_scenarios_table
./scripts/test-fast.sh -p trembita-dashboard --lib b35_
./scripts/test-fast.sh -p trembita-http --lib b35_
./scripts/test-fast.sh -p trembita --test ingress_lb_ops b35_
```

Ops: [ingress-lb § B-35](../ops/ingress-lb.md#join-readiness-pipeline-b-35) · ADR: [cluster-elasticity § B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35) · Runbook: [production-runbook § Join readiness](../ops/production-runbook.md#join-readiness-b-35).

## R3 directory visibility (B-36)

Actor directory is **eventually consistent (R3)**. After rebalance or scale, **`merge_lag_epochs`** on **`GET /introspect/directory-r3`** shows how far the merged view trails local publish; **`deliver_no_target_totals`** accumulates cross-node delivers that still returned **`NoTarget`** after RYW retries. Product capability apps default to **`read_your_writes`** (**8 × 25 ms**); post-rebalance boost is visible as **`directory_retry_boost_active`**.

| Scenario | Regression |
|----------|------------|
| Cumulative `NoTarget` per worker group | `b36_no_target_totals_accumulate_per_group` |
| Optional hook on each `NoTarget` record | `b36_on_no_target_hook_invoked_per_record` |
| Merge lag vs peer epochs (three-row table) | `b36_merge_lag_scenarios_table` |
| Reopen keeps live sticky target when still registered | `b36_reopen_reuses_live_session_before_rekeying` |
| Reopen re-picks after registration dropped | `b36_reopen_keyed_picks_after_target_gone` |
| Reopen returns none when group empty | `b36_reopen_returns_none_when_group_has_no_registrations` |
| Introspect JSON matches `TrembitaApp::directory_r3_snapshot()` | `b36_introspect_directory_r3_reports_ryw_defaults` |
| Default ops table serves `/introspect/directory-r3` | `b36_product_ops_only_exposes_directory_r3_route` |
| Inline capability traffic during multi-Raft adopt; lag bounded | `capability_inline_survives_raft_group_rebalance` |

**App recovery:** long-lived workflows call [`ActorSession::reopen_str`](../../crates/trembita-runtime/src/session.rs) or gateway [`SessionHandle::reopen`](../../crates/trembita/src/gateway/session.rs) after migration — do not assume the old `ActorId` remains valid.

```bash
./scripts/test-fast.sh -p trembita-runtime --lib b36_
./scripts/test-fast.sh -p trembita --test directory_r3_report b36_
./scripts/test-fast.sh -p trembita --lib capability_inline_survives_raft_group_rebalance
```

ADR: [actor-routing § B-36](../decisions/actor-routing.md#r3-visibility--sticky-recovery-b-36) · [structural-limits § R3](structural-limits.md#r3--actor-directory-is-eventually-consistent) · Runbook: [production-runbook § R3](../ops/production-runbook.md#r3-directory-visibility-b-36).

## Product surface gaps (B-41)

Three assembly-level features exposed on **`TrembitaApp`** for advanced apps (most capability-first services skip these).

| Surface | Product API | When |
|---------|-------------|------|
| **Durable mailbox** | [`TrembitaConfigure::with_durable_mailbox`](../../crates/trembita/src/configure.rs) or [`.with_durable_mailbox(true)`](../../crates/trembita/src/app/builder.rs) + **`TREMBITA_DATA_DIR`** | Cross-node [`UserActor`](../../crates/trembita-runtime/src/registry/actor.rs) / `/actor/deliver` — creates `{data_dir}/mailbox-spool.redb` |
| **Leader loops** | [`TrembitaAppBuilder::on_leader`](../../crates/trembita/src/app/builder.rs) + [`LeaderLoopOpts`](../../crates/trembita-runtime/src/leader_task.rs) | Custom reconcile / bootstrap on Raft leader only ([leader-task](../decisions/leader-task.md#product-surface-b-41)) |
| **Postgres schedules** | Feature **`schedule-postgres`** → [`PgScheduleSource`](../../crates/trembita-schedule-postgres/src/lib.rs) via [`.schedule_source`](../../crates/trembita/src/app/manifest.rs) on [`AppManifest`](../../crates/trembita/src/app/manifest.rs) | DB-backed recurring jobs ([schedule-source § B-41](../decisions/schedule-source.md#postgres-adapter-b-41)) |

**Prefer capabilities** for new product work — durable mailbox is for legacy/advanced actor paths, not default queued caps.

### Automated regression (B-41)

| Scenario | Regression |
|----------|------------|
| Configure flag true/false/default | `b41_configure_durable_mailbox_scenarios_table` |
| Flag applies through `TrembitaConfigure::apply_to` | `b41_durable_mailbox_applies_to_cluster_builder` |
| Boot creates `mailbox-spool.redb` | `b41_durable_mailbox_boot_creates_spool_redb` |
| Builder `.with_durable_mailbox` ≡ configure | `b41_builder_with_durable_mailbox_matches_configure_flag` |
| `on_leader` ticks while leader | `b41_on_leader_ticks_while_raft_leader` |
| Manifest `.schedule_source` chains into builder | `b41_manifest_schedule_source_chains_into_builder` |
| Product boot with static schedule source | `b41_schedule_source_manifest_boots_without_panic` |
| Invalid SQL table ident rejected | `b41_rejects_invalid_table_ident` |
| SQL ident validation table | `b41_sql_ident_validation_scenarios_table` |
| Default Postgres schema column names | `b41_default_schema_matches_documented_columns` |

```bash
./scripts/test-fast.sh -p trembita --lib b41_
./scripts/test-fast.sh -p trembita-schedule-postgres --lib b41_
./scripts/test-fast.sh -p trembita --test app_cluster b41_
./scripts/test-fast.sh -p trembita --test schedule_source b41_
```

Getting started: [§ Durable mailbox + leader tasks](../getting-started.md#durable-mailbox--leader-tasks-b-41) · Runbook: [production-runbook § B-41](../ops/production-runbook.md#advanced-product-surface-b-41).

## Queued idempotency

At-least-once still applies; use **three layers** together ([idempotency-contract](../decisions/idempotency-contract.md)):

1. **Enqueue** — `CallBuilder::dedup_key`, or `CapRequest::cap_key()` from `#[cap_handler(key = "field")]`, or HTTP `?dedup=` on [`cap_enqueue`](../../crates/trembita/src/gateway/cap_handlers.rs).
2. **Bridge** — when `TREMBITA_DATA_DIR` enables [`TrembitaApp::actor_state_store`](../../crates/trembita/src/app/runtime.rs) (cap store), the cap queue consumer runs [`IdempotencyOpts::by_dedup_key`](../../crates/trembita/src/consumer.rs) around delivery (`cap:{stream}:` prefix).
3. **Handler** — domain markers in store for partial failure before ack (see [background-jobs](background-jobs.md#effectively-once-recipe)).

## Related

- [capability-parity](capability-parity.md) — scenario matrix (same power, no `UserActor` in app)
- [stateful-workers](stateful-workers.md) — showcase (capabilities + migration demo)
- [background-jobs](background-jobs.md) — queue semantics for queued routes
- [framework-conventions](../decisions/framework-conventions.md) — `capabilities/` layout
