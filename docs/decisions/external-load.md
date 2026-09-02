# External compute load — weighted tokens and load port

**Status:** Accepted (implemented)  
**Epic:** [B-17](../backlog.md#b-17--external-compute-load)  
**Extends:** [workload-governor](workload-governor.md)

## Context

[`ComputeTokenPool`](../../crates/crafty-actor/src/compute_token.rs) is **cooperative and in-process** — the ADR states this honestly. That breaks down for the workloads teams colocate with API for night-time utilisation:

- Browser automation (Chromium via CDP)
- ffmpeg / image pipelines
- Any `Command::spawn` / shell-out

The async handler **holds one token** while waiting on IO, but the CPU that competes with the gateway runs in a **child process the pool cannot see**. Accounting lies both ways: the token is busy when the tokio task is idle, and real load is invisible.

## Decision

Two complementary mechanisms — not either/or:

| Mechanism | API | Role |
|-----------|-----|------|
| **Static reservation** | `JobOpts::compute_cost(n)` | Handler acquires `n` token units for its whole run — reserves capacity for subprocess work |
| **Dynamic signal** | `ExternalLoad` port on `WorkloadOpts` | Application reports live subprocess pressure; governor treats it like ingress |

### Weighted token acquire

```rust
JobOpts::new("google-parser")
    .compute_cost(4)
    .consumer(&ParseConsumer)
```

[`ComputeTokenPool::acquire_weighted`](../../crates/crafty-actor/src/compute_token.rs) decrements `in_use` by `n` on drop. Gateway and actor ask remain at weight `1`.

On an 8-token pool, at most two `compute_cost(4)` handlers run concurrently — even if each handler spends most of its time awaiting CDP IO.

### External load port

```rust
WorkloadOpts::balanced()
    .external_load(Arc::new(my_child_process_tracker))
```

```rust
pub trait ExternalLoad: Send + Sync {
    fn units(&self) -> usize;
}
```

[`run_workload_governor`](../../crates/crafty-actor/src/workload.rs) maps `units()` into **effective connection pressure**:

`effective_connections = gateway_connections + external_units × api_protect_connections / max_compute_tokens`

When external load is high, the governor publishes protective consumer tune and lowers the token ceiling — same path as hot ingress.

Optional test helper: [`ManualExternalLoad`](../../crates/crafty-actor/src/external_load.rs).

## Consequences

- Teams declare subprocess cost once per stream; no custom governor wiring for the common case
- Dynamic port covers variable child counts (pool of browsers, cgroup CPU, …) without over-reserving tokens during idle IO
- Weighted acquire is still cooperative — runaway subprocesses need OS limits; the port only informs fairness
- `compute_cost` applies per stream via [`ConsumerOpts`](../../crates/crafty/src/consumer.rs); low-level `.consumer()` callers set it explicitly

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Separate OS process for jobs only | Rejected — contradicts homogeneous-node story |
| Cgroup integration in core | Rejected — platform-specific; belongs in app adapter implementing `ExternalLoad` |
| `compute_cost` only | Insufficient when load varies within one handler (browser pool) |
| `ExternalLoad` only | Rejected — most teams know static cost; one line beats a port |

## References

- [external-backlog](external-backlog.md) — same port pattern for backlog outside tier C
- [background-jobs](../scenarios/background-jobs.md) — consumer tuning and workload governor
