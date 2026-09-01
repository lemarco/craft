# Background jobs (background jobs)

Sidekiq-style async work on crafty: clients get **HTTP 202**, jobs survive restarts in redb, workers lease/ack through the replicated queue.

## What you run

| Piece | Role |
|-------|------|
| This binary | `CraftyApp` + gateway + `#[consumer]` email worker |
| [`trigger.sh`](trigger.sh) | Enqueue via HTTP (product gateway, not admin) |
| Admin (local) | `:9080` — dashboard in single-process mode |
| Admin (cluster) | `:9180` on node 1 — dashboard in cluster mode |

## Quick start (local — one terminal)

**Terminal 1** — start the app:

```bash
cd examples/background-jobs
cargo run --release
```

**Terminal 2** — enqueue work:

```bash
./trigger.sh welcome-user-42
./trigger-batch.sh 30
```

## Quick start (cluster — 3–4 terminals, QUIC)

Three **identical** nodes — each runs HTTP gateway + queue consumer:

```bash
cd examples/background-jobs
./cluster.sh setup
./cluster.sh up        # or: ./cluster.sh 1 | 2 | 3 in separate terminals

./trigger-batch.sh 30
```

Open **http://127.0.0.1:9180/dashboard** — event feed shows workers on different nodes as jobs are leased.

| Node | QUIC | Admin | Gateway |
|------|------|-------|---------|
| 1 | `:7543` | `:9180` | `:8090` |
| 2 | `:7553` | `:9181` | `:8091` |
| 3 | `:7563` | `:9182` | `:8092` |
| 4 | `:7573` | `:9183` | — (optional) |

`./trigger-batch.sh` round-robins across `:8090`, `:8091`, `:8092` — each gateway forwards enqueue to the queue leader; any node can lease and process jobs.

You should see `[worker] email #… sent` in server terminals and the **Job queues** panel move on [http://127.0.0.1:9180/dashboard](http://127.0.0.1:9180/dashboard).

## curl equivalents

```bash
curl -X POST http://127.0.0.1:8090/jobs/emails \
  -H 'content-type: application/json' \
  -d '{"payload":"manual-curl"}'
```

Batch:

```bash
curl -X POST http://127.0.0.1:8090/jobs/emails/batch \
  -H 'content-type: application/json' \
  -d '{"jobs":[{"payload":"a"},{"payload":"b"}]}'
```

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `CRAFTY_GATEWAY` | `127.0.0.1:8090` (local) | Product HTTP bind (`-` disables gateway on a node) |
| `CRAFTY_WORKERS` | `1` | Consumer instances **on this node only** (local dev) |
| `CRAFTY_PEERS` | unset | When set → QUIC cluster mode (`cluster.sh`) |
| `CRAFTY_DATA_DIR` | `/tmp/crafty-showcase-background-jobs` | redb queue + actor store |

## Troubleshooting

**`ERR_CONNECTION_REFUSED` in the browser**

1. **Same machine** — run `curl` / `./trigger.sh` in a terminal on the host where `cargo run` is running, not only in a browser on another machine.
2. **SSH / remote dev** — forward ports: `ssh -L 8090:127.0.0.1:8090 -L 9180:127.0.0.1:9180 user@host`
3. **Port busy** — startup now prints `crafty: gateway listening on http://…` or panics; try `CRAFTY_GATEWAY=127.0.0.1:8091 cargo run`
4. **Wrong URL** — `/jobs/emails` is **POST only** (enqueue). For monitoring use [dashboard](http://127.0.0.1:9180/dashboard) in cluster mode (or `:9080` in local mode); a GET in the browser returns `405`, not a HTML page.

Verify:

```bash
curl -sf -X POST http://127.0.0.1:8090/jobs/emails \
  -H 'content-type: application/json' -d '{"payload":"ping"}' -w '\nHTTP %{http_code}\n'
```

## Production shape

Same binary on every VPS; scale by adding nodes. Each node can accept HTTP and run consumers — the replicated queue assigns work. For edge-only ingress (no local consumers), set `CRAFTY_ROLE=gateway` via the library env API.

Guide: [docs/scenarios/background-jobs.md](../../docs/scenarios/background-jobs.md)
