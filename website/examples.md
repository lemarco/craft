# Product showcases

Five standalone projects in the repository — each with its own `Cargo.toml`, README, `cluster.sh` (QUIC/mTLS), and `trigger.sh`.

| Showcase | Pattern | Source |
|----------|---------|--------|
| **background-jobs** | HTTP `202` → queue → `#[consumer]` | [examples/background-jobs](https://gitlab.com/lemarco/trembita/-/tree/main/examples/background-jobs) |
| **realtime** | WebSocket + HTTP → sticky `ActorSession` | [examples/realtime](https://gitlab.com/lemarco/trembita/-/tree/main/examples/realtime) |
| **stateful-workers** | Capabilities + `ActorStateStore` idempotency + `POST /orders/submit` | [examples/stateful-workers](https://gitlab.com/lemarco/trembita/-/tree/main/examples/stateful-workers) |
| **workflows** | Saga journal + actor/queue steps | [examples/workflows](https://gitlab.com/lemarco/trembita/-/tree/main/examples/workflows) |
| **self-update** | Leader-coordinated rolling self-update | [examples/self-update](https://gitlab.com/lemarco/trembita/-/tree/main/examples/self-update) |

## Quick run (solo node)

```sh
git clone git@gitlab.com:lemarco/trembita.git
cd trembita
./scripts/run-example.sh background-jobs
```

Other showcases: replace `background-jobs` with `realtime`, `stateful-workers`, `workflows`, or `self-update`.

## Multi-node cluster

From a trembita repo checkout, contributors can use the **debug** CLI (`cargo build -p trembita-cli` → `./target/debug/trembita dev up --showcase …`). Release installs from crates.io do not include `dev`.

Each example also ships a `cluster.sh` that boots several QUIC peers with shared mTLS:

```sh
cd examples/background-jobs
./cluster.sh up
./trigger.sh
./cluster.sh down
```

Every showcase runs the **same binary on every node** — gateway + workers/consumers on each VPS. The cluster routes work (Raft leader, queue lease, actor directory).

## Docker Compose

Pre-built compose stacks under `dev/compose/`:

| Showcase | Command |
|----------|---------|
| background-jobs | `cd dev/compose/background-jobs && docker compose up --build` |
| stateful-workers | `cd dev/compose/stateful-workers && docker compose up --build` |
| realtime | `cd dev/compose/realtime && docker compose up --build` |
| workflows | `cd dev/compose/workflows && docker compose up --build` |

## Reading the code

Start with [`background-jobs/src/main.rs`](https://gitlab.com/lemarco/trembita/-/blob/main/examples/background-jobs/src/main.rs) — heavily commented module docs explain **what**, **why**, and **how** (solo vs multi-node). Other showcases follow the same layout.

## Scenario guides

Each showcase maps to a scenario guide:

- [Background jobs](/scenarios/background-jobs)
- [Real-time sessions](/scenarios/realtime-sessions)
- [Stateful workers](/scenarios/stateful-workers)
- [Workflows](/scenarios/workflows)

Full index: [examples/README.md on GitLab](https://gitlab.com/lemarco/trembita/-/blob/main/examples/README.md).
