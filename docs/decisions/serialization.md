# Wire serialization — postcard

**Status:** Accepted  
**Date:** 2026-07-05

## Context

HTTP/3 request/response bodies need a binary encoding for Rust types in `raft-proto` (peer RPC, client API, and optionally snapshot chunks). We considered postcard, CBOR, bincode, and JSON.

## Decision

Use **`postcard`** with **`serde`** for all hot-path wire bodies.

| Use | Encoding |
|-----|----------|
| `POST /raft/v1/peer/wire` | `postcard(PeerWireMessage)` |
| `POST /raft/v1/client/wire` | `postcard(ClientRequest)` / `postcard(ClientResponse)` |
| State machine command/query payloads | User types via `serde` + postcard inside `ClientRequest::payload` |
| Log entry `data` field | Opaque bytes (user-defined; typically postcard of command type) |

**HTTP header:** `Content-Type: application/x-postcard`

**Crate:** `postcard` (workspace dependency in `raft-proto`)

## API (raft-proto)

```rust
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, EncodeError>;
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError>;
```

Centralize in `crates/raft-proto/src/codec.rs` so `raft-net` and `raft-client` do not depend on postcard directly for every type.

## Why postcard

- Compact binary wire format; no protobuf or JSON overhead
- Works with standard `#[derive(Serialize, Deserialize)]`
- `no_std`-friendly — aligns with lean dependencies
- Single codec for peer RPC and client API

## Alternatives rejected

| Option | Why not |
|--------|---------|
| **JSON** | Larger, slower; poor fit for Raft hot path |
| **Protobuf** | Rejected with gRPC; schema/codegen workflow |
| **bincode** | Viable; postcard chosen for compactness and embedded pedigree |
| **CBOR** | More self-describing than needed; slightly heavier |

## Consequences

- **Positive:** One codec crate-wide; types live in Rust only
- **Negative:** Not self-describing — wire compatibility requires matching Rust types and field order
- **Negative:** Non-Rust clients must implement postcard or use a bridge (out of scope v1)

Optional **dev-only JSON** for debugging may be added later; it is not the default wire format.

## Related

- [protocol.md](../protocol.md)
- [wire-transport.md](wire-transport.md)
- [client-api.md](client-api.md)
- [state-machine.md](state-machine.md)
