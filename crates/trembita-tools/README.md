# trembita-tools

Workspace binaries and shared dev helpers (`publish = false`).

| Binary | Purpose |
|--------|---------|
| `trembita-node` | Reference **product** node (`TrembitaApp` + empty SM) from env/CLI; ops vars `TREMBITA_PEERS` / `TREMBITA_NODE_ID` → [`EnvOverrides`](../../crates/trembita-assembly/src/env_config.rs) via [`into_app_config`](src/node/config.rs) — see [env.md](../../docs/env.md) |
| `trembita-ops` | Snapshot backup/restore; **`upgrade run`** — HTTP rolling upgrade operator (B-54) |
| `trembita-e2e-client` | Linearizability load generator |
| `trembita-e2e-queue-client` | Queue E2E smoke client |
| `trembita-e2e-elastic` | Product elastic + LB docker E2E app (B-34) |
| `trembita-dev-client` | Dev QUIC client |
| `trembita-showcase-client` | Product showcase HTTP/WS helper |

Build: `cargo build -p trembita-tools --release --bin trembita-node`

**Upgrade operator (B-54):** `cargo build -p trembita-tools --release --bin trembita-ops` → `trembita-ops upgrade run --help`. Docs: [capabilities § B-54](../../docs/scenarios/capabilities.md#ci-cluster-upgrade-operator-b-54).
