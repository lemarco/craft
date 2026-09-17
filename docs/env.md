# Product environment variables

[`TrembitaApp::from_env()`](../crates/trembita/src/app/runtime.rs) and [`RunOpts::from_env()`](../crates/trembita/src/app_opts.rs) read a **small product surface**. Everything else is optional (jobs, auth) or **advanced / ops-only** (static clusters, `trembita-node`, e2e).

## Product boot chain

Typical product binary (see [getting-started.md](getting-started.md), [examples/](../examples/README.md)):

```rust
use trembita::{AppManifest, TrembitaApp, TrembitaConfigure};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::init_tracing();
    TrembitaApp::from_env()?
        .manifest(app_manifest()) // `.jobs`, `.workers`, `.capabilities`, …
        .gateway_routes(/* optional cap routes */)
        .configure(
            TrembitaConfigure::default()
                .with_data_dir(/* or rely on TREMBITA_DATA_DIR */)
                .with_local_gateway_apis(), // dev/tests; prod often uses default ops from env
        )
        .run()
        .await
}
```

When `main` already parsed env (embedder, [`trembita-node`](../crates/trembita-tools/src/bin/node.rs), tests):

```rust
use trembita::{AppManifest, RunOpts, TrembitaApp};
use trembita::env::AppConfig;

let cfg: AppConfig = /* trembita::env::app_config_from_env()? or trembita_tools::node::config::config_from_env()?.into_app_config(seeds) */ ;
let manifest = app_manifest();
TrembitaApp::from_config(cfg)
    .manifest(manifest)
    .run_with(RunOpts::for_manifest(&cfg, &manifest))
    .await?;
```

Register domain wiring in [`AppManifest`](../crates/trembita/src/app/manifest.rs) — not duplicate `.jobs()` / `.queue()` on the builder for product code ([public-api-1.0](decisions/public-api-1.0.md)).

**Custom Raft state machines** are not part of this path — see [public-api-1.0](decisions/public-api-1.0.md) (`trembita-showcase` / in-crate `integration` only).

## Product apps (typical deploy)

| Variable | Required | Purpose |
|----------|----------|---------|
| [`TREMBITA_LISTEN`](#trembita_listen) | yes (default `0.0.0.0:443`) | **One port number:** QUIC wire (UDP) + product/ops HTTP (TCP) on the same `host:port` |
| [`TREMBITA_DATA_DIR`](#trembita_data_dir) | yes for queues + cap store | redb (`queue-*`, `actor-store.redb` / cap store), snapshots, persisted **`node-id`** after join |
| [`TREMBITA_CERT_DIR`](#trembita_cert_dir) | yes in prod (or `dev-certs` feature locally) | Shared dir: `ca.pem`, `node-{id}.pem` — joiners boot with `node-0.pem`, reload after id assign |
| [`TREMBITA_JOIN_SEEDS`](#trembita_join_seeds) | joiners only | `id@host:port` of a seed (`1@node1:443`) — **no static peer mesh** in the happy path |
| [`GATEWAY_TOKEN`](#gateway_token) | optional | Bearer + `X-Trembita-User` for identity-protected `/jobs/*` and sticky routing ([gateway-identity](decisions/gateway-identity.md)) |
| [`TREMBITA_GATEWAY_SESSION_SECRET`](#trembita_gateway_session_secret) | multi-node gateway + cookies | Same secret on every node — cluster session cookies ([gateway-cluster-auth](decisions/gateway-cluster-auth.md)) |

Common optional:

| Variable | Purpose |
|----------|---------|
| `TREMBITA_JOB_QUEUE` | Job stream name when using env-only queue registration + [`RunOpts::from_config`](../crates/trembita/src/app_opts.rs) / [`RunOpts::for_manifest`](../crates/trembita/src/app_opts.rs) wait-for-leader |
| `TREMBITA_JOB_QUEUE_SHARDS` | Fixed physical shard count for env-only `TREMBITA_JOB_QUEUE` (`{name}~0` …); requires `TREMBITA_DATA_DIR`. Mutually exclusive with `TREMBITA_JOB_QUEUE_AUTO_SHARD` |
| `TREMBITA_JOB_QUEUE_AUTO_SHARD` | `1` / `true` — leader adaptive shard growth for env-only queue ([job-queue](decisions/job-queue.md)) |
| `TREMBITA_RAFT_GROUPS` | Multi-Raft coordination groups on the product **`EmptyStateMachine`** path (default `1`). Use with `TREMBITA_DATA_DIR` for keyed queue/topic/store traffic ([multi-raft](decisions/multi-raft.md)) |
| `TREMBITA_RAFT_SHARD_COUNT` | Virtual shard modulus when `TREMBITA_RAFT_GROUPS` > 1 (optional; assembly default applies when unset) |
| `TREMBITA_ALLOW_JOIN` | Seed accepts dynamic join (default **on** when not joining) |

**Do not set** `TREMBITA_NODE_ID` on product nodes — id comes from join assignment and `{data_dir}/node-id`.

### `TREMBITA_LISTEN`

Single published port per node. Wire and HTTP share the port **number** (different protocols). Default gateway surfaces bind here automatically when using [`from_env()`](../crates/trembita/src/app/runtime.rs) / [`from_config()`](../crates/trembita/src/app/builder.rs) (ops + registration-driven APIs; not `/actors/*`).

**Load balancing:** point your edge (DNS, floating IP, reverse proxy) at **each** node’s TCP listener on this port; use **`GET /ready`** for pool health. Inter-node QUIC uses **UDP** on the same port between real node IPs — see [ops/ingress-lb.md](ops/ingress-lb.md).

**Ops (zero config):** `/health`, `/ready`, `/metrics`, `/dashboard`, `/introspect/*` on the same listener — no manual `http::ops` merge. Opt out with [`.without_ops()`](../crates/trembita/src/app/builder.rs) only when ops live on another host.

**Product APIs (registration-driven):** on the same listener when using [`from_env()`](../crates/trembita/src/app/runtime.rs) / default gateway surfaces:

| Registration | Routes (identity-protected by default) | Opt-out |
|--------------|----------------------------------------|---------|
| [`.jobs([…]).http_enqueue(true)`](../crates/trembita/src/job_opts.rs) | `POST/GET /jobs/*`, `GET/PUT/DELETE /jobs/{stream}/schedules/*` | [`.without_jobs_api()`](../crates/trembita/src/app/builder.rs), [`.without_schedules_api()`](../crates/trembita/src/app/builder.rs) |
| [`.topics([…])`](../crates/trembita/src/app/builder.rs) | `POST /topics/{name}/publish`, `GET /topics/{name}` | [`.without_topics_api()`](../crates/trembita/src/app/builder.rs) |
| [`.workflows([…])`](../crates/trembita/src/app/builder.rs) | `POST /workflows/run`, `POST /workflows/resume` | [`.without_workflows_api()`](../crates/trembita/src/app/builder.rs) |
| [`.workers()`](../crates/trembita/src/worker_opts.rs) + [`.http_cast(true)`](../crates/trembita/src/worker_opts.rs) | `/actors/*` (advanced) | Default off; scaffolds call [`.without_actors_api()`](../crates/trembita/src/app/builder.rs) |

**Greenfield product HTTP:** declare routes in `src/http/product.rs` with [`cap_fire` / `cap_invoke`](../crates/trembita/src/gateway/cap_handlers.rs) — see [capability-greenfield-wire](decisions/capability-greenfield-wire.md).

Declare capabilities in [`AppManifest`](../crates/trembita/src/app/manifest.rs) (`src/manifest.rs` in scaffolded apps). Custom routes still merge via [`.gateway_routes()`](../crates/trembita/src/app/builder.rs); explicit [`.gateway().surfaces()`](../crates/trembita/src/gateway/opts.rs) is merged with these defaults automatically.

### `TREMBITA_DATA_DIR`

Enables durable job queue, **cap store** (`trembita::capstore`), and node id persistence. Required when `TREMBITA_JOB_QUEUE` is set. Sharded / multi-Raft coordination env vars also require a data directory.

**Manifest alternative (B-32):** [`.sharded(n)`](../crates/trembita/src/queue_opts.rs) / [`.auto_shard()`](../crates/trembita/src/queue_opts.rs) on [`QueueOpts`](../crates/trembita/src/queue_opts.rs) / [`JobOpts`](../crates/trembita/src/job_opts.rs); [`.with_coordination_raft_groups(n)`](../crates/trembita/src/configure.rs) / [`.with_coordination_shard_count(n)`](../crates/trembita/src/configure.rs) on [`TrembitaConfigure`](../crates/trembita/src/configure.rs). Runtime catalog expansion: [`TrembitaApp::add_raft_groups`](../crates/trembita/src/app/runtime.rs).

### `TREMBITA_CERT_DIR`

After dynamic join, the runtime loads `node-{assigned_id}.pem` from this directory ([certs.md](certs.md)). Prefer this over per-file `TREMBITA_NODE_*` paths.

Local solo dev: enable crate feature **`dev-certs`** and omit cert env — ephemeral mTLS is generated.

### `TREMBITA_JOIN_SEEDS`

Comma-separated seeds for joiners. Seed nodes omit this and set `TREMBITA_ALLOW_JOIN=1` (default when not joining).

### `GATEWAY_TOKEN`

Also accepted as `TREMBITA_GATEWAY_TOKEN` (legacy name). Unset = open product HTTP (dev only).

### `TREMBITA_GATEWAY_SESSION_SECRET`

Shared signing key for product session cookies (≥16 bytes). Every gateway process in the cluster must use the **same** value so [`SessionGate`](../crates/trembita-http/src/routing/auth.rs) accepts cookies on any node. Alias: `GATEWAY_SESSION_SECRET`. See [gateway-cluster-auth](decisions/gateway-cluster-auth.md) and [`ClusterSessionSecret`](../crates/trembita/src/gateway/cluster_session.rs).

### `TREMBITA_GATEWAY_AUTH_PROFILE`

`cookie-only` (default) or `external-idp` — composition hint for [`GatewayAuthProfile`](../crates/trembita/src/gateway/auth_profile.rs); OAuth/OIDC stays app-owned via [`.identity()`](../crates/trembita/src/gateway/opts.rs).

---

## Do not use in product deploys

| Variable | Status |
|----------|--------|
| `TREMBITA_HTTP`, `TREMBITA_GATEWAY` | **Internal only:** `-` disables TCP (QUIC-only node). Any other value must equal `TREMBITA_LISTEN` — **omit** in normal deploys. |
| `TREMBITA_NODE_ID` | **Ops / static clusters** (`trembita-node`, fixed voter bootstrap). Product apps: use `node-id` file. |
| `TREMBITA_PEERS` | **Static voter bootstrap** — fixed id→address map at first boot; use join seeds for elastic clusters. |
| `TREMBITA_NODE_CERT`, `TREMBITA_NODE_KEY`, `TREMBITA_CA_CERT` | Low-level PEM paths — use `TREMBITA_CERT_DIR` + `node-{id}.pem` instead. |
| `TREMBITA_GATEWAY_*` (API toggles) | **Do not use** — routes come from app registration / default gateway surfaces. |
| `TREMBITA_ADMIN`, split admin ports | **Do not use** — [unified-listener](decisions/unified-listener.md) |

---

## Advanced (same binary, explicit tuning)

| Variable | Purpose |
|----------|---------|
| `TREMBITA_JOIN_ROLE` | `learner` (default) or `voter` (needs seed `TREMBITA_ALLOW_VOTER_JOIN=1`) |
| `TREMBITA_ALLOW_VOTER_JOIN` | Seed accepts voter joins (default `0`) |
| `TREMBITA_HTTP_TLS_CERT` / `TREMBITA_HTTP_TLS_KEY` | HTTPS on the unified TCP listener (`TREMBITA_GATEWAY_TLS_*` aliases) |
| `TREMBITA_CERT_WATCH_SECS` | PEM hot-reload poll (default `60`) |
| `TREMBITA_HTTP_DRAIN_TIMEOUT` | Gateway connection drain on shutdown (default 30s; alias `TREMBITA_GATEWAY_DRAIN_TIMEOUT`) |
| `TREMBITA_GRACEFUL_LEAVE`, `TREMBITA_DRAIN_TIMEOUT`, … | Shutdown / cluster policy — see [`trembita::env`](../crates/trembita/src/env.rs) / [`env_config.rs`](../crates/trembita-assembly/src/env_config.rs) |

---

## Reference binaries

| Binary | Notes |
|--------|--------|
| Product app | Table above |
| [`trembita-node`](../crates/trembita-tools/src/bin/node.rs) | Reference **product** node (`TrembitaApp` + empty SM): may use `TREMBITA_NODE_ID`, `TREMBITA_PEERS`, `TREMBITA_DISCOVERY`, `--join-seed`. [`NodeConfig::into_app_config`](../crates/trembita-tools/src/node/config.rs) sets [`EnvOverrides`](../crates/trembita-assembly/src/env_config.rs) (e.g. `env.peers` when `TREMBITA_PEERS` is set) so static membership merges into the builder — **not** for normal app deploys |
| [`dev-client`](../crates/trembita-tools/src/bin/dev-client.rs) | Client tooling; requires `TREMBITA_PEERS` |

Run `trembita doctor` on scaffold projects — it checks `manifest.rs` ↔ `consumers/` wiring, gateway merges in `app.rs`, legacy keys in `deploy/.env.example`, **removed APIs** (`TrembitaCluster::builder`, old gateway toggles), and **capability scale foot-guns** (B-31 — e.g. `.instances(1)` with queued ops but no keyed handlers). Before deploy, use **`trembita doctor --preflight`**: stricter checks for `TREMBITA_LISTEN` / `DATA_DIR` / `CERT_DIR`, compose join pattern (no `TREMBITA_NODE_ID`), default ops gateway wiring, and local `deploy/certs/ca.pem` when present.

**Scale wave env (B-28–B-32):** B-29 — [`TREMBITA_GATEWAY_SESSION_SECRET`](#trembita_gateway_session_secret); B-30 — pool health via **`GET /ready`** on [`TREMBITA_LISTEN`](#trembita_listen) ([ingress-lb](ops/ingress-lb.md)); B-32 — `TREMBITA_JOB_QUEUE_*`, `TREMBITA_RAFT_*` (table above). Cap group defaults (B-28) are manifest-side, not env. Index: [status § Product scale wave](status.md#product-scale-wave-b-28b32).

See also: [getting-started.md](getting-started.md), [certs.md](certs.md), [unified-listener](decisions/unified-listener.md).
