# trembita-tools

Workspace binaries and shared dev helpers (`publish = false`).

| Binary | Purpose |
|--------|---------|
| `trembita-node` | Reference cluster node from env/CLI |
| `trembita-ops` | Snapshot backup/restore |
| `trembita-e2e-client` | Linearizability load generator |
| `trembita-e2e-queue-client` | Queue E2E smoke client |
| `trembita-dev-client` | Dev QUIC client |
| `trembita-showcase-client` | Product showcase HTTP/WS helper |

Build: `cargo build -p trembita-tools --release --bin trembita-node`
