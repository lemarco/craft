# Seed vs joiner environment matrix (B-45)

Product **elastic** clusters: same binary, per-host `TREMBITA_DATA_DIR` and assigned `node-id` file after join. See [env.md](../../docs/env.md) for semantics.

| Variable | Seed (first node) | Joiner (node 2+) | Notes |
|----------|-------------------|------------------|-------|
| `TREMBITA_LISTEN` | Required | Required | Same port number on every VPS (TCP + UDP) |
| `TREMBITA_DATA_DIR` | Required | Required | Unique path per host |
| `TREMBITA_CERT_DIR` | Required | Required | Seed: `node-1.pem` (or assigned id). Joiner: **`node-0.pem`** until join, then `node-<id>.pem` |
| `TREMBITA_ALLOW_JOIN` | `1` (default when not joining) | Omit | Seed accepts join RPCs |
| `TREMBITA_JOIN_SEEDS` | **Omit** | Required | e.g. `1@10.0.0.11:443` — use **real IP** for QUIC, not only LB VIP |
| `TREMBITA_JOIN_ROLE` | N/A | `learner` (default) or `voter` | Voter join needs seed `TREMBITA_ALLOW_VOTER_JOIN=1` |
| `TREMBITA_ALLOW_VOTER_JOIN` | Optional `1` | N/A | Off by default |
| `TREMBITA_NODE_ID` | **Do not set** | **Do not set** | Persisted under `data_dir` after join |
| `TREMBITA_PEERS` | **Do not set** | **Do not set** | Static bootstrap only (`trembita-node`, e2e) |
| `TREMBITA_GATEWAY_SESSION_SECRET` | Same on all nodes | Same | Required for cookie auth behind LB ([B-40](../../docs/decisions/gateway-cluster-auth.md)) |
| `GATEWAY_TOKEN` | Optional | Optional | Protects product HTTP when set |
| `TREMBITA_HTTP_TLS_*` | Optional | Optional | Node TLS when clients hit nodes directly |
| `TREMBITA_DRAIN_TIMEOUT` | Recommended | Recommended | Actor/cap drain; systemd `TimeoutStopSec` ≥ this + gateway drain |
| `TREMBITA_HTTP_DRAIN_TIMEOUT` | Recommended | Recommended | Gateway connection drain on shutdown |
| `TREMBITA_GRACEFUL_LEAVE` | `1` typical | `1` typical | Raft leave on SIGTERM when in cluster |

Examples: [seed.env.example](seed.env.example) · [joiner.env.example](joiner.env.example).

**LB vs cluster wire:** HTTP clients use the edge pool ([ingress-lb](../../docs/ops/ingress-lb.md)); **UDP QUIC** between members uses node IPs — firewall must allow peer UDP on `TREMBITA_LISTEN`.
