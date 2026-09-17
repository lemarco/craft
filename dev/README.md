# Dev tooling (not product showcases)

**Primary dev UX (repo contributors):** build the **debug** [`trembita` CLI](../crates/trembita-cli/README.md) — **`trembita dev` is not in `cargo build --release` or crates.io installs**.

```bash
cargo build -p trembita-cli
./target/debug/trembita dev setup --showcase stateful-workers
./target/debug/trembita dev up --showcase stateful-workers --nodes 3
./target/debug/trembita dev trigger stateful-workers -- 1001
./target/debug/trembita dev http --showcase background-jobs -- job emails hello
./target/debug/trembita dev cluster-up --setup   # B-39: shared session secret + smoke hints
```

See [getting-started §4](../docs/getting-started.md#4-try-the-showcases).

Infrastructure below supports legacy `./cluster.sh`, CI compose, and CA demos. Product scenarios live in [`examples/`](../examples/) only.

| Path | Purpose |
|------|---------|
| [`certs/generate.sh`](certs/generate.sh) | Mint dev/small-prod mTLS PKI (`openssl` only) |
| [`step-ca/`](step-ca/) | Optional step-ca docker-compose + renewal demo |
| [`3node/README.md`](3node/README.md) | Live 3-node `trembita-node` cluster — `./scripts/dev-3node.sh` |
| [`founder-3node/README.md`](founder-3node/README.md) | **Founder** 3-node product cluster (B-39) — session smoke + optional nginx `:18290`; docs [capabilities § B-39](../docs/scenarios/capabilities.md#local-3-node-founder-cluster-b-39) |
| [`cluster-common.sh`](cluster-common.sh) | Shared `cluster.sh` helpers (certs, build, `./cluster.sh up`) |
| [`compose/`](compose/) | One-command Docker Compose clusters per showcase |
| [`gitlab-runner/`](gitlab-runner/) | Self-hosted GitLab CI runner (Docker on local PC) |

## Docker Compose (CI / demo — not primary dev path)

| Showcase | Command |
|----------|---------|
| background-jobs | `cd dev/compose/background-jobs && docker compose up --build` |
| stateful-workers | `cd dev/compose/stateful-workers && docker compose up --build` |
| realtime | `cd dev/compose/realtime && docker compose up --build` |
| workflows | `cd dev/compose/workflows && docker compose up --build` |

## Local QUIC cluster (legacy `./cluster.sh`)

Each showcase still provides `./cluster.sh` (used by scripts; or use `./target/debug/trembita dev`):

```bash
cd examples/background-jobs
./cluster.sh setup    # certs + release build + showcase client
./cluster.sh up       # start 3 nodes in background
./cluster.sh health
./cluster.sh logs 1   # tail node 1 log
./cluster.sh stop
```

Product apps from **`trembita new`** use **`trembita doctor`** (read-only) — not `trembita dev`.
