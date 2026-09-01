# GitLab Runner (local Docker)

Self-hosted runner for [`.gitlab-ci.yml`](../../.gitlab-ci.yml) on a machine with Docker.

## Setup

```bash
cd dev/gitlab-runner
cp .env.example .env
# Edit .env — paste the runner token from GitLab (Settings → CI/CD → Runners).
./register.sh
docker compose up -d
```

Verify in GitLab: **Settings → CI/CD → Runners** — the runner should show as online (green).

## Usage

- Tag: **`shared`** — set in GitLab when creating the runner (glrt tokens).
- `run_untagged: true` — picks up jobs without tags (configured in GitLab UI).
- Jobs that use `services: docker:dind` need a runner with Docker; this setup mounts the host socket and runs privileged containers.

## Operations

```bash
docker compose logs -f          # runner logs
docker compose restart          # after config.toml edits
docker compose down             # stop runner (jobs queue until back online)
```

Re-register (new token): `rm -rf config && ./register.sh && docker compose up -d`

## Files

| Path | Purpose |
|------|---------|
| `.env` | Token and runner name (gitignored) |
| `config/` | `config.toml` + credentials (gitignored) |
| `register.sh` | One-shot registration via ephemeral container |
| `docker-compose.yml` | Long-lived runner service |
