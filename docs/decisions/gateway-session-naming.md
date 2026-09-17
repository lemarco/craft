# Gateway sticky session naming (worker, not actor)

**Status:** Accepted  
**Date:** 2026-09-17  
**Related:** [product-terminology](product-terminology.md), [gateway-identity](gateway-identity.md), [realtime-sessions](../scenarios/realtime-sessions.md)

## Context

Product realtime and capability [`Route::Session`](../../crates/trembita/src/capability/mod.rs) use **sticky worker routing** (`SessionHandle`, capability groups). Legacy HTTP/WebSocket helpers were named **`open_actor_session_*`** and **`OpenActorSessionError`**, which implied app-authored **`UserActor`** — not the greenfield product path.

## Decision

Rename public gateway session errors and open helpers to **worker** vocabulary:

| Before (deprecated 0.7.0) | After |
|---------------------------|--------|
| `OpenActorSessionError` | `OpenWorkerSessionError` |
| `TrembitaGatewayState::open_actor_session` | `open_worker_session` |
| `open_actor_session_from` | `open_worker_session_from` |
| `open_actor_session_parts` | `open_worker_session_parts` |

Runtime wire types (`ActorSession`, directory) unchanged — they remain in `trembita-runtime`.

Deprecated aliases and method wrappers remain for **one release** (`since = "0.7.0"`) then may be removed pre-1.0.

## Doc inventory

Update product paths that referenced `open_actor_session_*`: [realtime-sessions](../scenarios/realtime-sessions.md), [gateway-routing-v2](gateway-routing-v2.md), [gateway-identity](gateway-identity.md).
