# E2E cluster (docker-compose)

A 3-node `craft` cluster over **real QUIC + mTLS**, exercised end to end in
containers. This is backlog **T8** — the highest-fidelity test in the suite: it
uses the actual `craft-node` binary, the actual cert-provisioning script, and a
real network between separate processes.

## Layout

- `Dockerfile` — builds the `craft-node` binary and a small runtime image
  (openssl for cert generation, curl for health polling). Build context is the
  repo root.
- `docker-compose.yml` — a one-shot `certgen` service mints a shared cluster CA
  and one cert per node into a shared volume, then `node1..node3` boot, discover
  each other by service DNS name, and elect a leader. mTLS still validates each
  peer against its `craft-node-<id>` SAN, independent of the container hostname.
- `run.sh` — brings the cluster up, asserts a single agreed leader is elected,
  kills that leader, asserts the survivors re-elect a new one, then tears
  everything down.
- `chaos.sh` — fault injection (T9): partitions the leader off the cluster
  network, asserts the majority re-elects, heals the partition, and asserts the
  whole cluster re-converges on one leader (no split brain). Also an opt-in
  latency scenario via [`pumba`](https://github.com/alexei-led/pumba).
- `cert_renew.sh` — PEM hot reload (cert-automation): reissues a follower's on-disk
  cert, triggers reload via **SIGHUP** and via **file poll**
  (`CRAFT_CERT_WATCH_SECS`), and asserts the cluster stays healthy without
  process restart.
- `lib.sh` — shared helpers (compose wrapper, leader polling) sourced by both.

## Run it

```sh
./e2e/run.sh                    # election + failover
./e2e/chaos.sh                  # partition + heal
./e2e/cert_renew.sh             # PEM reissue + SIGHUP/poll hot reload
CRAFT_E2E_PUMBA=1 ./e2e/chaos.sh  # also inject 250ms±50ms latency via pumba
```

Requires Docker + `docker compose`. Admin APIs are published on host ports
`18081` (node 1), `18082` (node 2), `18083` (node 3); e.g.
`curl localhost:18081/introspect/cluster`.

Under GitLab dind the published ports live on the `docker` service host, so the
CI job sets `CRAFT_E2E_HOST=docker`.

## Chaos (T9)

`chaos.sh` implements network partition + heal (dependency-free, via
`docker network disconnect/connect`) and an opt-in `pumba` latency scenario
(`CRAFT_E2E_PUMBA=1`) that adds delay + jitter to every node and asserts
consensus survives. `run.sh`'s leader-kill is a third fault scenario.
