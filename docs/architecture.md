# Architecture overview

Multi-node **Raft** cluster in Rust: **one `ractor` actor per peer**, **pure `RaftCore` FSM**, I/O at the edges (network, storage, client).

> Decision records in [decisions/](decisions/) override details here as they are accepted.

## Stack (current decisions)

| Concern | Choice | Decision |
|---------|--------|-----|
| State machine | Generic trait + macros | [state-machine](decisions/state-machine.md) |
| Wire transport | **HTTP/3 + QUIC** (all network I/O) | [wire-transport](decisions/wire-transport.md) |
| Serialization | **postcard** + serde | [serialization](decisions/serialization.md) |
| Client API | L1 `ractor`; L2 HTTP/3 remote | [client-api](decisions/client-api.md) |
| ~~gRPC/tonic~~ | Rejected | [client-api](decisions/client-api.md) |
| ~~Framed TCP~~ | Rejected | [wire-transport](decisions/wire-transport.md) |
| Deployment | Library-first framework; one app, N VPS processes | [deployment-model](decisions/deployment-model.md) |
| Elasticity | Incremental join + local/runtime actor scale | [elastic-cluster](decisions/elastic-cluster.md) |
| Cross-node actors | Messaging, spawn_remote, scale_cluster, migration | [cross-node-actors](decisions/cross-node-actors.md) |
| Worker placement | **1 worker / VPS**; auto-spawn on join | [one-worker-per-vps](decisions/one-worker-per-vps.md), [auto-spawn-on-join](decisions/auto-spawn-on-join.md) |
| Discovery | `JOIN_ADDR` + **joint-consensus membership** (v1) | [discovery](decisions/discovery.md), [membership-early](decisions/membership-early.md) |
| Client routing | Transparent forward (any node) | [client-routing](decisions/client-routing.md) |
| Read consistency | ReadIndex linearizable `query` | [read-consistency](decisions/read-consistency.md) |
| Actor runtime | `ractor` + `tokio` | — |
| Persistence | `redb` | — |
| Naming | **`craft`** facade + `craft-*` crates | [naming](decisions/naming.md) |
| TLS | mTLS peers + **mTLS client wire**; in-process exempt | [security](decisions/security.md) |
| Observability | metrics, telemetry, introspection, dashboard | [observability](decisions/observability.md) |

## Crate layout

```
crates/
├── craft/           # facade — primary user dependency
├── craft-proto/     # IDs, log, peer + client wire types
├── craft-core/      # Pure Raft FSM (input/output effects)
├── craft-storage/   # LogStore, HardState, Snapshot (+ redb)
├── craft-net/       # HTTP/3 server, QUIC peer pool, router
├── craft-actor/     # CraftNodeActor, ActorRegistry, directory, migration
├── craft-client/    # ClientHandle, RemoteClient, TypedClient
├── craft-macros/    # State machine + UserActor derives
├── craft-node/      # Reference binary
├── craft-sim/       # In-memory Transport for partition tests
├── craft-store-redis/  # optional: ActorStateStore Redis impl (actor-state-redis)
└── craft-dashboard/    # optional: live monitoring UI (observability)
```

See [naming](decisions/naming.md).

## Node internals

```mermaid
flowchart TB
    subgraph Node["One process"]
        RC[RaftCore]
        RNA[RaftNodeActor]
        SM[StateMachine]
        ST[Storage]
        H3[HTTP/3 server :7443/udp]

        H3 -->|/peer/wire| RNA
        H3 -->|/client/wire| RNA
        H3 -->|/actor/deliver| AR[User actors]
        H3 -->|/actor/register| DIR[Actor directory]
        DIR --> AR
        RNA --> RC
        RC --> SM
        RNA --> ST
        RNA -->|outbound peer RPC| H3
    end

    H3 <-->|QUIC peer/wire| Peers[Other nodes]
    H3 <-->|QUIC client/wire| Clients[Remote clients]
    H3 <-->|QUIC actor/deliver| Peers
```

One **QUIC listener** per node. Raft replication and client requests share the HTTP/3 stack; paths separate peer vs client auth.

## Client API layers

```mermaid
flowchart LR
    subgraph L1["L1 In-process"]
        APP[Your app]
        CH[ClientHandle]
        AR[RaftNodeActor]
        APP --> CH --> AR
    end

    subgraph L2["L2 Remote HTTP/3"]
        APP2[CLI / other binary]
        RMC[RemoteClient]
        QUIC[QUIC + POST /client/wire]
        AR2[RaftNodeActor]
        APP2 --> RMC --> QUIC --> AR2
    end
```

## Data flows

### Write

1. Client sends `ClientRequest::Propose` to **any** node (L1 or L2).
2. If receiver is a follower, it **forwards** to the leader ([client-routing](decisions/client-routing.md)).
3. Leader appends to log, persists, replicates via `AppendEntries` over **`POST /peer/wire`**.
4. Majority match → commit → `StateMachine::apply`.
5. `ClientResponse::Ok` returned to caller (directly or proxied).

### Election

Follower election timeout → Candidate → `RequestVote` over HTTP/3 → majority → Leader → heartbeats as `AppendEntries` on `/peer/wire`.

### Read

1. Client sends `ClientRequest::Query` to any node (forwarded to leader if needed).
2. Leader runs **ReadIndex** (quorum ack + apply barrier) → `StateMachine::query`.
3. `ClientResponse::Ok` returned. Actor `ask` is not linearizable — see [read-consistency](decisions/read-consistency.md).

## Decisions complete; implementation topics

Strategic decision records through **cluster-routing** are accepted. Optional medium topics in [open-questions.md](open-questions.md).

## Implementation phases

1. Workspace scaffold  
2. `craft-proto` + `craft-core` — election, replication, **joint-consensus membership** ([membership-early](decisions/membership-early.md))  
3. `craft-storage`  
4. `craft-net` — HTTP/3, **`/cluster/join`**, peer pool  
5. `craft-actor` + `craft` facade + auto-spawn supervisor ([auto-spawn-on-join](decisions/auto-spawn-on-join.md))  
6. `craft-client` + snapshots + `craft-macros`  
7. `craft-sim` — membership, partition, join/leave tests
