# Product environment variables

[`TrembitaApp::from_env()`](../crates/trembita/src/app/runtime.rs) and [`RunOpts::from_env()`](../crates/trembita/src/app_opts.rs) read a **small product surface**. Everything else is optional (jobs, auth) or **advanced / ops-only** (static clusters, `trembita-node`, e2e).

## Product apps (typical deploy)

| Variable | Required | Purpose |
|----------|----------|---------|
| [`TREMBITA_LISTEN`](#trembita_listen) | yes (default `0.0.0.0:443`) | **One port number:** QUIC wire (UDP) + product/ops HTTP (TCP) on the same `host:port` |
| [`TREMBITA_DATA_DIR`](#trembita_data_dir) | yes for queues/actors | redb, snapshots, persisted **`node-id`** after join |
| [`TREMBITA_CERT_DIR`](#trembita_cert_dir) | yes in prod (or `dev-certs` feature locally) | Shared dir: `ca.pem`, `node-{id}.pem` — joiners boot with `node-0.pem`, reload after id assign |
| [`TREMBITA_JOIN_SEEDS`](#trembita_join_seeds) | joiners only | `id@host:port` of a seed (`1@node1:443`) — **no static peer mesh** in the happy path |
| [`GATEWAY_TOKEN`](#gateway_token) | optional | Bearer + `X-Trembita-User` for identity-protected `/jobs/*` and sticky routing ([gateway-identity](decisions/gateway-identity.md)) |

Common optional:

| Variable | Purpose |
|----------|---------|
| `TREMBITA_JOB_QUEUE` | Job stream name when using env-only queue registration + [`RunOpts::from_config`](../crates/trembita/src/app_opts.rs) / [`RunOpts::for_manifest`](../crates/trembita/src/app_opts.rs) wait-for-leader |
| `TREMBITA_ALLOW_JOIN` | Seed accepts dynamic join (default **on** when not joining) |

**Do not set** `TREMBITA_NODE_ID` on product nodes — id comes from join assignment and `{data_dir}/node-id`.

### `TREMBITA_LISTEN`

Single published port per node. Wire and HTTP share the port **number** (different protocols). Default gateway surfaces bind here automatically when using [`from_env()`](../crates/trembita/src/app/runtime.rs).

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

Enables durable job queue, actor store, and node id persistence. Required when `TREMBITA_JOB_QUEUE` is set.

### `TREMBITA_CERT_DIR`

After dynamic join, the runtime loads `node-{assigned_id}.pem` from this directory ([certs.md](certs.md)). Prefer this over per-file `TREMBITA_NODE_*` paths.

Local solo dev: enable crate feature **`dev-certs`** and omit cert env — ephemeral mTLS is generated.

### `TREMBITA_JOIN_SEEDS`

Comma-separated seeds for joiners. Seed nodes omit this and set `TREMBITA_ALLOW_JOIN=1` (default when not joining).

### `GATEWAY_TOKEN`

Also accepted as `TREMBITA_GATEWAY_TOKEN` (legacy name). Unset = open product HTTP (dev only).

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
| `TREMBITA_GRACEFUL_LEAVE`, `TREMBITA_DRAIN_TIMEOUT`, … | Shutdown / cluster policy — see [`env_config.rs`](../crates/trembita/src/env_config.rs) |

---

## Reference binaries

| Binary | Notes |
|--------|--------|
| Product app | Table above |
| [`trembita-node`](../crates/trembita-tools/src/bin/node.rs) | May still use `TREMBITA_PEERS` + explicit node id for KV demos |
| [`dev-client`](../crates/trembita-tools/src/bin/dev-client.rs) | Client tooling; requires `TREMBITA_PEERS` |

Run `trembita doctor` on scaffold projects — it checks `manifest.rs` ↔ `consumers/` wiring, gateway merges in `app.rs`, and legacy keys in `deploy/.env.example`. Before deploy, use **`trembita doctor --preflight`**: stricter checks for `TREMBITA_LISTEN` / `DATA_DIR` / `CERT_DIR`, compose join pattern (no `TREMBITA_NODE_ID`), default ops gateway wiring, and local `deploy/certs/ca.pem` when present.

See also: [getting-started.md](getting-started.md), [certs.md](certs.md), [unified-listener](decisions/unified-listener.md).
