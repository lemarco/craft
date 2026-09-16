# Self-update showcase

Leader-coordinated rolling upgrade ([upgrade-coordinator](../../docs/decisions/upgrade-coordinator.md)) on a 3-node QUIC cluster.

## Quick start

```bash
./cluster.sh setup
./cluster.sh up          # background nodes (TREMBITA_UPGRADE_DRY_RUN=1 default)
./trigger-upgrade.sh     # POST manifest → rolling grant on each node
```

Dry-run mode reports `Ready` without process exit. For production-style restarts, unset `TREMBITA_UPGRADE_DRY_RUN` and run under **systemd** with `Restart=always`.

## HTTP

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/cluster/upgrade` | Fleet rolling status |
| `POST` | `/cluster/upgrade/desired` | Start rolling (`202`) |

## Ports (local `./cluster.sh`)

| Node | `TREMBITA_LISTEN` (wire + upgrade HTTP) |
|------|----------------------------------------|
| 1 | 8190 |
| 2 | 8191 |
| 3 | 8192 |

## Workspace binary

Nodes run the unpublished showcase crate (custom [`UpgradeMachine`](../../crates/trembita-core/src/upgrade.rs)):

```bash
cargo run -p trembita-showcase --bin showcase-self-update
```

The `examples/self-update` crate is a thin wrapper for `trembita dev` / cluster scripts.

## Related

- [docs/ops/rolling-upgrade.md](../../docs/ops/rolling-upgrade.md)
- [docs/decisions/upgrade-coordinator.md](../../docs/decisions/upgrade-coordinator.md)
- [facade-layering](../../docs/decisions/facade-layering.md)
