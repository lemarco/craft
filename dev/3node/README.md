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
| Admin snapshot | HTTP → node **1** | Prints `/introspect/cluster` and `/introspect/queues` |

All writes go over **QUIC/mTLS** (ports 7443/7453/7463). Admin HTTP is read-only.

## Dashboard

| Node | Admin port | URL |
|------|------------|-----|
| 1 | 9080 | http://127.0.0.1:9080/dashboard |
| 2 | 9081 | http://127.0.0.1:9081/dashboard |
| 3 | 9082 | http://127.0.0.1:9082/dashboard |

Any node shows the same cluster state. Path must be `/dashboard` (not `/`).

### SSH from another machine

Browser `127.0.0.1` is your **laptop**, not the server. Either:

```bash
# Port-forward all three admin ports
ssh -L 9080:127.0.0.1:9080 -L 9081:127.0.0.1:9081 -L 9082:127.0.0.1:9082 lecomp
```

Or restart nodes after `./scripts/dev-3node.sh setup` — admin binds `0.0.0.0` by default
and open `http://<server-tailscale-ip>:9080/dashboard`.

## Admin HTTP (read-only)

```bash
curl http://127.0.0.1:9080/health
curl http://127.0.0.1:9080/introspect/cluster
curl http://127.0.0.1:9080/introspect/queues
curl http://127.0.0.1:9080/metrics
```

Works on **any** node (9080/9081/9082).

## Manual QUIC client

The demo binary [`trembita-dev-client`](../../crates/trembita-dev-client/) can be run directly:

```bash
cargo build -p trembita-dev-client --release
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

- [`trembita-node` README](../../crates/trembita-node/README.md)
- [`docs/scenarios/background-jobs.md`](../../docs/scenarios/background-jobs.md) — product HTTP (`TrembitaApp`)
- [`examples/README.md`](../../examples/README.md) — product showcases (in-process)
