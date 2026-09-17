# Production deploy pack (B-45)

VPS / bare-metal templates for **one product binary per node** — no Kubernetes charts in this repo ([deployment-model](../docs/decisions/deployment-model.md)).

| Artifact | Purpose |
|----------|---------|
| [systemd/trembita.service](systemd/trembita.service) | Unit template (`EnvironmentFile`, drain-friendly `TimeoutStopSec`) |
| [env/seed.env.example](env/seed.env.example) | First voter / seed node |
| [env/joiner.env.example](env/joiner.env.example) | Elastic joiner (learner default) |
| [env/matrix.md](env/matrix.md) | Seed vs joiner variable matrix |
| [nginx/upstream.conf.example](nginx/upstream.conf.example) | HTTP LB with `GET /ready` |
| [Dockerfile](Dockerfile) | Reference **`trembita-node`** image (not typical product apps) |
| [rolling-upgrade-systemd.md](rolling-upgrade-systemd.md) | Manual roll + upgrade-coordinator hooks |
| [backup-restore.md](backup-restore.md) | Stop-safe `data_dir` + cert export (B-48) |

**Runbook:** [production-runbook § B-45](../docs/ops/production-runbook.md#production-deploy-pack-b-45) · **Env reference:** [env.md](../docs/env.md) · **Certs:** [certs.md](../docs/certs.md) · **Ingress:** [ingress-lb.md](../docs/ops/ingress-lb.md)

## Recommended filesystem layout

Use the same paths on every node; only `EnvironmentFile` and `TREMBITA_DATA_DIR` differ per host.

```text
/opt/myapp/
  current -> releases/myapp-1.2.3    # symlink swap for upgrades
  releases/myapp-1.2.3               # your TrembitaApp binary
/etc/myapp/
  trembita.env                       # copied from env/*.example, host-specific
/var/lib/myapp/
  data/                              # TREMBITA_DATA_DIR (redb + node-id)
  certs/                             # TREMBITA_CERT_DIR (ca.pem, node-*.pem)
```

Install the unit:

```bash
sudo install -d -o root -g root -m 0755 /etc/myapp
sudo cp deploy/env/seed.env.example /etc/myapp/trembita.env   # edit in place
sudo sed -e 's|@APP_USER@|myapp|g' \
        -e 's|@APP_INSTALL@|/opt/myapp/current|g' \
        -e 's|@ENV_FILE@|/etc/myapp/trembita.env|g' \
        -e 's|@APP_WORKDIR@|/opt/myapp|g' \
        deploy/systemd/trembita.service \
        | sudo tee /etc/systemd/system/myapp.service
sudo systemctl daemon-reload
sudo systemctl enable --now myapp.service
```

Mint cluster PKI before first boot ([dev/certs/generate.sh](../dev/certs/generate.sh)):

```bash
./dev/certs/generate.sh --node-id 1 --out /var/lib/myapp/certs
# Joiners: include node-0.pem for bootstrap, then reload node-<assigned>.pem after join
./dev/certs/generate.sh --node-id 0 --out /var/lib/myapp/certs --ca ...   # join bootstrap
```

## Preflight

From your app repo (with `deploy/.env.example`):

```bash
trembita doctor --preflight
```

Scaffold apps: see [trembita-cli templates](../crates/trembita-cli/templates/trembita-app/deploy/) for Compose-first local deploy; use **this directory** when moving to systemd on real VPS.

## What trembita does not ship

- Ansible/Terraform modules (bring your own on top of these templates)
- Managed cloud LB configs — map provider health checks to **`GET /ready`** ([ingress-lb](../docs/ops/ingress-lb.md))
- Automatic backup — use [backup-restore.md](backup-restore.md) + [docs/ops/backup-restore.md](../docs/ops/backup-restore.md) (B-48)
