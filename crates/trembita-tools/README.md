# trembita-tools

Workspace binaries and shared dev helpers (`publish = false`).

| Binary | Purpose |
|--------|---------|
| `trembita-node` | Reference **product** node (`TrembitaApp` + empty SM) from env/CLI; ops vars `TREMBITA_PEERS` / `TREMBITA_NODE_ID` — see [env.md](../../docs/env.md) |
| `trembita-ops` | Snapshot backup/restore |
| `trembita-e2e-client` | Linearizability load generator |
| `trembita-e2e-queue-client` | Queue E2E smoke client |
| `trembita-dev-client` | Dev QUIC client |
| `trembita-showcase-client` | Product showcase HTTP/WS helper |

Build: `cargo build -p trembita-tools --release --bin trembita-node`
