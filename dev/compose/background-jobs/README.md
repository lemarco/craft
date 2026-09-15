# Background jobs — Docker Compose cluster

Dynamic join (no `TREMBITA_NODE_ID`) — same model as `./examples/background-jobs/cluster.sh`.

```bash
docker compose up --build
```

## Endpoints (node 1)

| URL |
|-----|
| http://127.0.0.1:8090/jobs/emails |
| http://127.0.0.1:8090/dashboard |
| http://127.0.0.1:8090/health |

Nodes 2 and 3: `:8091`, `:8092` — same binary (gateway + consumer after join).

## Enqueue a job

```bash
curl -X POST http://127.0.0.1:8090/jobs/emails \
  -H 'content-type: application/json' \
  -d '{"payload":"hello-from-compose"}'
```

Or with the internal showcase client (after `cargo build -p trembita-showcase-client`):

```bash
./target/debug/trembita-showcase-client job 127.0.0.1:8090 emails hello
```

## Layout

- **certgen** — CA + join bootstrap (`node-0.pem`) + `node-1..3.pem`
- **node1** — seed (`TREMBITA_ALLOW_JOIN=1`)
- **node2/3** — `TREMBITA_JOIN_SEEDS=1@node1:8090`

Image: [`examples/background-jobs/Dockerfile`](../../../examples/background-jobs/Dockerfile).
