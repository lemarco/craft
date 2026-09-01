# State machine API

**Status:** Accepted  
**Date:** 2026-07-05

## Context

The cluster needs a user-defined state machine applied after log entries commit. We must decide whether the repo ships only a KV example or a reusable API for arbitrary machines.

## Decision

Ship a **generic trait + macros** for user-defined state machines. A minimal reference KV lives in [`crafty_core::kv`](../../crates/crafty-core/src/kv.rs) (re-exported as `crafty::kv`); product showcases under [`examples/`](../../examples/README.md) demonstrate Raft SM, actor mailbox, and job-queue patterns.

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

## Implementation note (2026-07-06)

The trait landed in `crafty-core` (`StateMachine`, with associated
`Command`/`Query`/`Response`/`Error`). The "Encode/Decode glue" and the
"owned & clone-safe" compile-time check are delivered **without a bespoke
derive**: `Command` and `Query` are marker traits with blanket impls over any
`serde` type that also satisfies the replication bounds (`Command: Clone + Send
+ 'static`, `Query: Send + 'static`). Users therefore just derive
`#[derive(Clone, Serialize, Deserialize)]` on their command/query types; a type
that borrows a lifetime or is not `Clone` fails to satisfy `Command` and will
not compile. This keeps the public macro surface smaller (a plus for a
published library, library-and-publishing), so the originally planned `StateMachine` derive is
not needed. Actor message ergonomics use the `remote_actor` attribute macro in `crafty-macros`.

## Consequences

- **Positive:** Embeddable in arbitrary domains (KV, counters, job queues, etc.)
- **Positive:** Examples document patterns without constraining the core
- **Negative:** Extra crate surface (trait bounds, macros, docs) before first runnable cluster
- **Negative:** Macro API must be stable early; breaking changes hurt downstream users

## Related

- [client-and-routing.md](client-and-routing.md) — how clients submit commands to the machine
- [architecture.md](../architecture.md) — `RaftCore` apply loop
