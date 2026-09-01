# Dev tooling (not product showcases)

Infrastructure for local QUIC clusters, certificates, and CA demos. Product scenarios live in [`examples/`](../examples/) only.

| Path | Purpose |
|------|---------|
| [`certs/generate.sh`](certs/generate.sh) | Mint dev/small-prod mTLS PKI (`openssl` only) |
| [`step-ca/`](step-ca/) | Optional step-ca docker-compose + renewal demo |
| [`3node/README.md`](3node/README.md) | Live 3-node `crafty-node` cluster — `./scripts/dev-3node.sh` |
| [`cluster-common.sh`](cluster-common.sh) | Shared `cluster.sh` helpers (certs, build, `./cluster.sh up`) |
| [`compose/`](compose/) | One-command Docker Compose clusters per showcase |
| [`gitlab-runner/`](gitlab-runner/) | Self-hosted GitLab CI runner (Docker on local PC) |

## Docker Compose showcases

| Showcase | Command |
|----------|---------|
| background-jobs | `cd dev/compose/background-jobs && docker compose up --build` |
| stateful-workers | `cd dev/compose/stateful-workers && docker compose up --build` |
| realtime | `cd dev/compose/realtime && docker compose up --build` |
| workflows | `cd dev/compose/workflows && docker compose up --build` |

## Local QUIC cluster (examples)

Each showcase provides `./cluster.sh`:

```bash
cd examples/background-jobs
./cluster.sh setup    # certs + release build + showcase client
./cluster.sh up       # start 3 nodes in background
./cluster.sh health
./cluster.sh logs 1   # tail node 1 log
./cluster.sh stop
```

Same pattern for `stateful-workers`, `realtime`, and `workflows`.
