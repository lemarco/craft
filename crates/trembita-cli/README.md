# trembita-cli

Framework CLI for [trembita](https://crates.io/crates/trembita) product apps — scaffold the
standard layout, register consumers/topics/actors/HTTP surfaces, and verify wiring.

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

trembita add consumer emails --lease 300
trembita add topic platform.events
trembita add actor catalog
trembita add http-surface api --hosts api.example.com
trembita add static-site app --hosts app.example.com --filesystem fe/app/dist

trembita doctor
trembita doctor --fix
```

See [`framework-conventions`](../../docs/decisions/framework-conventions.md) for the generated
project layout and [facade ADR](../../docs/decisions/facade.md) for how adapter features map to `trembita`.

## Relation to `trembita-tools`

[`trembita-tools`](../trembita-tools/) remains **workspace-only** (`publish = false`) and bundles
internal binaries (`trembita-node`, `trembita-ops`, e2e/showcase clients). Product developers
install **`trembita-cli`** only.
