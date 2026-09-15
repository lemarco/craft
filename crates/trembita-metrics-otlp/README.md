# trembita-metrics-otlp

Optional OpenTelemetry metrics adapter for trembita ([observability §2 export](../../docs/decisions/observability.md)).

- [`init_metrics_with_otlp`](src/lib.rs) — install a global OTLP/gRPC meter provider (binary boot).
- Enable on the facade with `features = ["otlp-metrics"]` or on `trembita-runtime` with `features = ["otlp"]`.

Pull metrics via `GET /metrics` remain the default; this crate is for push/export backends only.
