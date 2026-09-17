# trembita-cli

Framework CLI for [trembita](https://crates.io/crates/trembita) product apps — scaffold the
standard layout (`src/manifest.rs` capability registry + `app.rs` gateway/run) and verify wiring
with read-only `trembita doctor`. Capability registration is **manual** in `manifest.rs` / `app.rs`
(copy from [`examples/`](../../examples/) or scaffold templates).

## Install

From this repository (0.4.0 development):

```bash
cargo install --path crates/trembita-cli
```

After the 0.4.0 release on crates.io:

```bash
cargo install trembita-cli
```

## Usage

```bash
trembita new my-service --features jobs,gateway,telemetry
trembita new my-service --trembita-path ../trembita   # local checkout

trembita doctor                 # read-only layout / wiring checks
trembita doctor --preflight   # deploy: env, compose, certs/listen, gateway ops

# From trembita repo root — local showcase clusters (debug `trembita` only; not in `--release`)
cargo build -p trembita-cli
./target/debug/trembita dev list
./target/debug/trembita dev setup --showcase stateful-workers
./target/debug/trembita dev up --showcase stateful-workers --nodes 3
./target/debug/trembita dev trigger stateful-workers -- 1001
./target/debug/trembita dev trigger background-jobs -- job emails hello   # built-in HTTP (no trigger.sh)
./target/debug/trembita dev http --showcase workflows -- workflow run onboard-42
./target/debug/trembita dev stop --showcase stateful-workers
./target/debug/trembita dev cluster-up --setup --nodes 4 --lb   # B-42 elastic smoke (realtime)
```

Local cluster scripts: [`scripts/local-cluster.sh`](../../scripts/local-cluster.sh) · [dev/local-3node](../../dev/local-3node/README.md). Regression: `./scripts/test-fast.sh -p trembita-cli --lib b39_` · `./scripts/test-fast.sh -p trembita-cli --lib b42_`.

See [`framework-conventions`](../../docs/decisions/framework-conventions.md) for the generated
project layout and [facade ADR](../../docs/decisions/facade.md) for how adapter features map to `trembita`.

## Command coverage

What each subcommand touches and where it is tested ([testing-coverage](../../docs/testing-coverage.md#framework-cli-trembita-cli)):

| Command | Writes / checks | Automated tests |
|---------|-----------------|-----------------|
| `new` | Full tree + `manifest.rs` | `tests/scaffold.rs` |
| `doctor` | `manifest.rs` ↔ handlers; `app.rs` `.manifest()` only (read-only) | `src/scaffold/doctor.rs`, `tests/{add_doctor,manifest_doctor}.rs` |
| `doctor --preflight` | `deploy/` env + gateway ops | unit cases in `doctor.rs` |
| `dev *` (debug CLI only) | Repo `examples/` showcases; `dev cluster-up` / `cluster-lb-up` (B-39/B-42) | `tests/dev.rs` (`b39_*`, `b42_*`), `src/dev/local_cluster.rs`, `src/dev/http.rs` |

Run locally: `./scripts/test-fast.sh -p trembita-cli` from the repo root.

## Relation to `trembita-tools`

[`trembita-tools`](../trembita-tools/) remains **workspace-only** (`publish = false`) and bundles
internal binaries (`trembita-node`, `trembita-ops`, e2e/showcase clients). Product developers
install **`trembita-cli`** only.
