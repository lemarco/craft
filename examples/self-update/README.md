# Self-update showcase

Leader-coordinated rolling upgrade ([upgrade-coordinator](../../docs/decisions/upgrade-coordinator.md)) on a 3-node QUIC cluster.

## Quick start

```bash
./cluster.sh setup
./cluster.sh up          # background nodes (CRAFTY_UPGRADE_DRY_RUN=1 default)
./trigger-upgrade.sh     # POST manifest → rolling grant on each node
```

Dry-run mode reports `Ready` without process exit. For production-style restarts, unset `CRAFTY_UPGRADE_DRY_RUN` and run under **systemd** with `Restart=always`.

## HTTP

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/cluster/upgrade` | Fleet rolling status |
| `POST` | `/cluster/upgrade/desired` | Start rolling (`202`) |

## Ports (local)

| Node | QUIC | Upgrade API | Admin |
|------|------|-------------|-------|
| 1 | 7643 | 8190 | 9280 |
| 2 | 7653 | 8191 | 9281 |
| 3 | 7663 | 8192 | 9282 |

## Related

- [docs/ops/rolling-upgrade.md](../../docs/ops/rolling-upgrade.md)
- [docs/decisions/upgrade-coordinator.md](../../docs/decisions/upgrade-coordinator.md)
