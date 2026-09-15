# 3-node QUIC cluster — hands-on example

Real **QUIC/mTLS** cluster on localhost: three `trembita-node` processes, no Docker,
no in-process simulator.

## Quick start

```bash
# 1. One-time setup (certs + build)
./scripts/dev-3node.sh setup

# 2. Three terminals — one node each
./scripts/dev-3node.sh 1
./scripts/dev-3node.sh 2
./scripts/dev-3node.sh 3

# 3. Fourth terminal — open dashboard, then demo or watch
./scripts/dev-3node.sh demo    # fast smoke (~3s)
./scripts/dev-3node.sh watch   # staged ~2+ min for dashboard
```

Wait ~5–10 s after starting nodes for leader election.

## What `watch` shows on dashboard (~2+ min)

| Phase | Dashboard |
|-------|-----------|
| Raft propose ×5 | **Cluster** → `commit index` растёт |
| Enqueue ×6 | **Job queues** → `pending` 1…6 |
| Lease / ack waves | `pending` ↓, `leased` ↑ then ↓ |
| Event feed | SSE-события кластера |

Пауза между шагами: 10 s (override: `TREMBITA_DEV_WATCH_PAUSE_SECS=15`).

## What `demo` does (fast)

| Step | Wire | Description |
|------|------|-------------|
| Raft propose ×3 | QUIC → node **1** | Increments built-in Demo counter |
| Raft query | QUIC → node **3** | Linearizable read from a different node |
| Enqueue ×3 | QUIC → node **1** | Stream `jobs` |
| Lease + ack | QUIC → node **2** | Follower worker consumes jobs |
| Ops snapshot | HTTP → node **1** | Prints `/introspect/cluster` and `/introspect/queues` |

Each node uses one port number for both QUIC (UDP) and ops HTTP (TCP): **7443**, **7453**, **7463**.

## Dashboard

| Node | `TREMBITA_LISTEN` (QUIC + ops HTTP) | URL |
|------|-------------------------------------|-----|
| 1 | `127.0.0.1:7443` | http://127.0.0.1:7443/dashboard |
| 2 | `127.0.0.1:7453` | http://127.0.0.1:7453/dashboard |
| 3 | `127.0.0.1:7463` | http://127.0.0.1:7463/dashboard |

Any node shows the same cluster state. Path must be `/dashboard` (not `/`).

### SSH from another machine

Browser `127.0.0.1` is your **laptop**, not the server. Either:

```bash
# Port-forward all three ops HTTP binds (same port numbers as QUIC)
ssh -L 7443:127.0.0.1:7443 -L 7453:127.0.0.1:7453 -L 7463:127.0.0.1:7463 lecomp
```

Or bind `0.0.0.0` on the server (`TREMBITA_DEV_LISTEN_BIND=0.0.0.0` is the default in
`dev-3node.sh`) and open `http://<server-ip>:7443/dashboard`.

## Ops HTTP (read-only)

```bash
curl http://127.0.0.1:7443/health
curl http://127.0.0.1:7443/introspect/cluster
curl http://127.0.0.1:7443/introspect/queues
curl http://127.0.0.1:7443/metrics
```

Works on **any** node (7443 / 7453 / 7463).

## Manual QUIC client

The demo binary [`trembita-dev-client`](../../crates/trembita-tools/) can be run directly:

```bash
cargo build -p trembita-tools --release --bin trembita-dev-client
# same env as ./scripts/dev-3node.sh demo — see scripts/dev-3node.sh client_env()
```

Optional overrides: `TREMBITA_DEMO_PROPOSE_NODE`, `TREMBITA_DEMO_QUERY_NODE`,
`TREMBITA_DEMO_SUBMIT_NODE`, `TREMBITA_DEMO_WORKER_PEER`.

## Failover smoke (e2e harness)

```bash
./scripts/dev-3node.sh queue-smoke   # before killing the leader
# kill leader node, restart it, then:
TREMBITA_E2E_QUEUE_PHASE=after_failover ./scripts/dev-3node.sh queue-smoke
```

## Files

| Path | Purpose |
|------|---------|
| `target/trembita-3node-dev/certs/` | Cluster CA + node 1–4 certs |
| `target/trembita-3node-dev/data/node-{1,2,3}/` | Persistent redb (Raft + queue) |

## See also

- [`trembita-node` README](../../crates/trembita-tools/README.md)
- [`docs/scenarios/background-jobs.md`](../../docs/scenarios/background-jobs.md) — product HTTP (`TrembitaApp`)
- [`examples/README.md`](../../examples/README.md) — product showcases (in-process)
- [unified listener](../../docs/decisions/unified-listener.md) — one port number, UDP + TCP
