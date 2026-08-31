# Background jobs — Docker Compose cluster

One command to start a 3-node QUIC cluster:

```bash
docker compose up --build
```

## Endpoints

| Service | URL |
|---------|-----|
| Gateway (node 1) | http://127.0.0.1:8090/jobs/emails |
| Admin dashboard | http://127.0.0.1:9180/dashboard |

Nodes 2 and 3 expose `:8091` and `:8092` — same binary, each runs gateway + consumer.

## Enqueue a job

```bash
curl -X POST http://127.0.0.1:8090/jobs/emails \
  -H 'content-type: application/json' \
  -d '{"payload":"hello-from-compose"}'
```

Or with the internal showcase client (after `cargo build -p crafty-showcase-client`):

```bash
./target/debug/crafty-showcase-client job 127.0.0.1:8090 emails hello
```

## Layout

- **node1**, **node2**, **node3** — identical processes (HTTP ingress + `#[consumer]` on each)

Same image as [`examples/background-jobs/Dockerfile`](../../../examples/background-jobs/Dockerfile).
