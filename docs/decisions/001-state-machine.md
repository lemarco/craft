# ADR 001: State machine API

**Status:** Accepted  
**Date:** 2026-07-05

## Context

The cluster needs a user-defined state machine applied after log entries commit. We must decide whether the repo ships only a KV example or a reusable API for arbitrary machines.

## Decision

Ship a **generic trait + macros** for user-defined state machines. The KV store lives under `examples/` as a reference implementation, not as the only supported machine.

## Design sketch

```rust
// crates/raft-core/src/traits.rs
pub trait StateMachine: Send {
    type Command: Clone + Send + Encode + Decode;
    type Query: Send + Encode + Decode;
    type Error: Send;

    fn apply(&mut self, index: LogIndex, cmd: Self::Command) -> Result<(), Self::Error>;
    fn query(&self, q: Self::Query) -> Result<Vec<u8>, Self::Error>;
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error>;
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error>;
}
```

Macro responsibilities (crate `raft-macros` or `raft-core` proc-macro):

- Derive or generate `Encode`/`Decode` glue for command/query types
- Optional `#[raft_state_machine]` for snapshot header framing (index, term, payload)
- Compile-time checks that command types are owned and clone-safe for replication

## Consequences

- **Positive:** Embeddable in arbitrary domains (KV, counters, job queues, etc.)
- **Positive:** Examples document patterns without constraining the core
- **Negative:** Extra crate surface (trait bounds, macros, docs) before first runnable cluster
- **Negative:** Macro API must be stable early; breaking changes hurt downstream users

## Related

- [002-client-api.md](002-client-api.md) — how clients submit commands to the machine
- [architecture.md](../architecture.md) — `RaftCore` apply loop
