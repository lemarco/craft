# Backup / restore (B-48) — operator cheat sheet

Full runbook: [docs/ops/backup-restore.md](../docs/ops/backup-restore.md).

## Schedule

- **Before** risky upgrades or catalog changes ([rolling-upgrade-systemd.md](rolling-upgrade-systemd.md)).
- **Regular** exports per node (each VPS has its **own** `TREMBITA_DATA_DIR`).

## One-liner (stop node first)

```bash
sudo systemctl stop myapp
./scripts/backup-data-dir.sh \
  --data-dir /var/lib/myapp/data \
  --archive /var/backups/myapp-$(hostname -s)-$(date -u +%Y%m%d).tar.gz \
  --cert-dir /var/lib/myapp/certs
sudo systemctl start myapp
```

Install `trembita-ops` on the host (same release as your app binary) or run the script from a checkout that can `cargo build -p trembita-tools --bin trembita-ops`.

## Restore same node

1. Stop service, empty target `data_dir`, `trembita-ops backup import`.
2. Restore certs + `/etc/myapp/trembita.env`.
3. Start → `curl -sf https://127.0.0.1:443/ready`.

Do **not** import one voter's backup onto another live member.

## Not automatic

No in-process snapshot API, no cross-node sync — ops owns object storage retention and restore drills.
