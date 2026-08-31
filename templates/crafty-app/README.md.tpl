# {{PROJECT_NAME}} — local 3-node crafty cluster (dev)

Three VPS-equivalent nodes sharing a dev CA. Generate certs first:

```bash
../dev/certs/generate.sh --ca-only --out ./certs
for id in 1 2 3; do
  ../dev/certs/generate.sh --node-id "$id" --out ./certs \
    --ca ./certs/ca.pem --ca-key ./certs/ca.key
done
```

Then: `docker compose up`

Each node uses embedded **redb** (`data_dir`) — no Redis.

See [crafty scenarios](https://gitlab.com/lemarco/craft/-/tree/master/docs/scenarios).
