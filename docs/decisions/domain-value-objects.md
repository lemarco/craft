# Domain value objects (`trembita-proto::value`)

**Status:** Accepted  
**Date:** 2026-09-15  
**Version:** 0.5.1+

## Context

Wire structs in `trembita-proto` historically used raw `String`, `u64`, and `Vec<u8>` for
names, ids, protocol versions, and opaque keys. That made invalid states representable
(empty stream names, oversized dedup keys, inconsistent worker encoding) and pushed
validation to scattered call sites.

## Decision

Introduce **validated value objects** in [`crates/trembita-proto/src/value.rs`](../../crates/trembita-proto/src/value.rs), re-exported from `trembita_proto` where appropriate. Wire compatibility is preserved with **`#[serde(transparent)]`** on newtypes (postcard/JSON field shapes unchanged).

### Naming & resources

| Type | Role | Validation |
|------|------|------------|
| `StreamName` | Job queue stream (`jobs`, `jobs~0`) | Non-empty, max 256, no control chars; `try_sharded(base, shard)` |
| `TopicName` | Event topic | Same |
| `SubscriptionName` | Topic subscription | Same |
| `ActorGroupName` | Actor pool / spawn group | Same |
| `StoreKey` | Actor-store UTF-8 key | Same |
| `RoutingKey` | Consistent-hash actor routing | Same |
| `AdvertiseAddr` | Join / peer book address | Same |

Helpers: [`parse_sharded_stream`](../../crates/trembita-proto/src/value.rs) splits `base~N`.

### Identifiers & protocol

| Type | Role |
|------|------|
| `NodeId`, `JobId`, `LeaseId`, `TopicEventId`, `TopicLeaseId` | Opaque numeric ids (wire + domain) |
| `ProtocolVersion` | Join/catalog wire version; `is_compatible`, `try_accept` |
| `WorkerId` | `{ node: NodeId, instance }`; `worker_id_from_wire` / `worker_id_to_wire` for legacy flat fields |

### Queue / time / limits

| Type | Role |
|------|------|
| `JobPriority` | `u8` priority |
| `MaxAttempts` | `0` = unlimited |
| `UnixMillis` | Wall time on wire; `IMMEDIATE` = `0` |
| `NotBefore` | Optional schedule visibility |
| `TtlSecs` | Actor-store TTL seconds (`0` = none) |

### Opaque bytes

| Type | Role | Validation |
|------|------|------------|
| `DedupKey` | Queue idempotency | Non-empty, max 4096 bytes |
| `TransactionId`, `RouteKey` | Cross-shard 2PC | Same |
| `SagaId` | Meta-Raft saga journal key | Same; `Ord` for in-memory registry |

### Core timing

| Type | Role |
|------|------|
| `LogicalTick` | Deterministic Raft ticks in `trembita-core::Config` |
| `Config::try_new` | Validates election/heartbeat bounds |

### Membership

[`Membership::validate_stable()`](../../crates/trembita-proto/src/raft.rs) checks non-empty voters, no duplicates, learners ∉ voters (stable configs).

### Multi-Raft

[`RaftGroupId::META`](../../crates/trembita-core/src/shard.rs) replaces raw `u32::MAX` for Meta-Raft.

## Usage

**Construct at boundaries** (HTTP handler, join builder, enqueue RPC):

```rust
use trembita_proto::{StreamName, ValueError};

let stream = StreamName::try_new("jobs")?;
let sub = SubscriptionName::try_from("workers")?;
```

**Wire decode** accepts transparent types without re-validation; **apply `try_new` / `try_accept`** before trusting user input.

**Storage adapters** may use `.0` / `.as_str()` when talking to redb or hash maps keyed by `String`.

## Consequences

- **Pros:** Single validation module; typed ids; fewer stringly-typed bugs; wire unchanged on the wire.
- **Cons:** More `.0` / `.as_str()` at storage boundaries; embedders constructing wire structs must wrap names (or use `TryFrom<&str>`).

## Related

- [wire-protocol](wire-protocol.md) — postcard + shared types
- [architecture-style](architecture-style.md) — `trembita-proto` as intentional shared kernel
- [naming](naming.md) — crate layout
