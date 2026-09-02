# E2E cluster (docker-compose)

A 3-node `crafty` cluster over **real QUIC + mTLS**, exercised end to end in
containers. This is the highest-fidelity test in the suite: it
uses the actual `crafty-node` binary, the actual cert-provisioning script, and a
real network between separate processes.

## Layout

- `Dockerfile` — builds the `crafty-node` binary and a small runtime image
  (openssl for cert generation, curl for health polling). Build context is the
  repo root.
- `docker-compose.yml` — a one-shot `certgen` service mints a shared cluster CA
  and one cert per node into a shared volume, then `node1..node3` boot, discover
  each other by service DNS name, and elect a leader. mTLS still validates each
  peer against its `crafty-node-<id>` SAN, independent of the container hostname.
- `run.sh` — brings the cluster up, asserts a single agreed leader is elected,
  probes `/health` and `/introspect/cluster` on every admin port, kills that
  leader, asserts the survivors re-elect a new one, then tears everything down.
- `leave.sh` — `CRAFTY_GRACEFUL_LEAVE=1` on node3: SIGINT and assert surviving
  peers drop the departed node from membership before exit.
- `chaos.sh` — fault injection (T9): partitions the leader off the cluster
  network, asserts the majority re-elects, heals the partition, and asserts the
  whole cluster re-converges on one leader (no split brain). Also an opt-in
  latency scenario via [`pumba`](https://github.com/alexei-led/pumba).
- `cert_renew.sh` — PEM hot reload (cert-automation): reissues a follower's on-disk
  cert, triggers reload via **SIGHUP** and via **file poll**
  (`CRAFTY_CERT_WATCH_SECS`), and asserts the cluster stays healthy without
  process restart.
- `linearizability.sh` — Jepsen-lite gate: crafty-sim checker sweep, then docker
  E2E with concurrent QUIC clients (`crafty-e2e-client` + `crafty_sim::History`)
  and partition chaos under admin poll.
- `queue.sh` — job queue over QUIC: enqueue on the leader, lease/ack on a
  follower (`crafty-e2e-queue-client`), kill the leader, drain the replicated
  backlog on the new leader.
- `gateway_jobs.sh` — HTTP jobs batch + auth through product gateway (in-process test).
- `queue_idempotency.sh` — `IdempotencyOpts` under redelivery + dedup key across leader failover (in-process tests).
- `lib.sh` — shared helpers (compose wrapper, leader polling, `run_linclient`,
  `run_queue_client`) sourced by E2E scripts.

## Run it

```sh
./e2e/run.sh                    # election + admin smoke + failover
./e2e/leave.sh                  # graceful leave (CRAFTY_GRACEFUL_LEAVE)
./e2e/queue.sh                  # job queue enqueue / follower worker / failover
./e2e/gateway_jobs.sh         # HTTP jobs via gateway (integration test)
./e2e/queue_idempotency.sh    # idempotency under redelivery
./e2e/chaos.sh                  # partition + heal
./e2e/cert_renew.sh             # PEM reissue + SIGHUP/poll hot reload
./e2e/linearizability.sh        # sim checker + docker QUIC linearizability
CRAFTY_E2E_PUMBA=1 ./e2e/chaos.sh  # also inject 250ms±50ms latency via pumba
```

Requires Docker + `docker compose`. Admin APIs are published on host ports
`18081` (node 1), `18082` (node 2), `18083` (node 3); e.g.
`curl localhost:18081/introspect/cluster`.

Under GitLab dind the published ports live on the `docker` service host, so the
CI job sets `CRAFTY_E2E_HOST=docker`.

## Chaos (T9)

`chaos.sh` implements network partition + heal (dependency-free, via
`docker network disconnect/connect`) and an opt-in `pumba` latency scenario
(`CRAFTY_E2E_PUMBA=1`) that adds delay + jitter to every node and asserts
consensus survives. `run.sh`'s leader-kill is a third fault scenario.
