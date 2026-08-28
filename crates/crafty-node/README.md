# crafty-node

Reference binary that runs a single
[crafty](https://crates.io/crates/crafty) cluster node from environment
variables.

Use it to smoke-test deployments, explore the admin dashboard, or as a template
for your own binary. Real applications embed `CraftyCluster` with their own
`StateMachine` and actors instead.

## Install

```bash
cargo install crafty-node
```

## Run locally

```bash
# Single-node dev cluster (throwaway dev CA, admin on :8080)
cargo run -p crafty-node

# Dashboard: http://127.0.0.1:8080/dashboard
```

Configuration is entirely via environment variables — see the `crafty-node`
crate docs (`CRAFTY_NODE_ID`, `CRAFTY_LISTEN`, `CRAFTY_ADMIN`, `CRAFTY_PEERS`,
`CRAFTY_DATA_DIR`, …).

## Documentation

- [docs.rs/crafty-node](https://docs.rs/crafty-node)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
