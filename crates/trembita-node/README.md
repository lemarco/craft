# trembita-node

Reference binary that runs a single
[trembita](https://crates.io/crates/trembita) cluster node from environment
variables.

**Not published to crates.io** (`publish = false`). Build from the repository
or use the e2e Docker image.

Use it to smoke-test deployments, explore the admin dashboard, or as a template
for your own binary. Real applications embed `TrembitaCluster` with their own
`StateMachine` and actors instead.

## Run locally

```bash
# Single-node dev cluster (throwaway dev CA, admin on :8080)
cargo run -p trembita-node

# Dashboard: http://127.0.0.1:8080/dashboard
```

Configuration is entirely via environment variables — see the `trembita-node`
crate docs (`TREMBITA_NODE_ID`, `TREMBITA_LISTEN`, `TREMBITA_ADMIN`, `TREMBITA_PEERS`,
`TREMBITA_DATA_DIR`, …).

## Repository

- [trembita on GitLab](https://gitlab.com/lemarco/trembita)
- [docs/certs.md](../../docs/certs.md) — mTLS provisioning

## License

Dual-licensed under `MIT OR Apache-2.0`.
