# trembita-metrics-otlp

Optional OpenTelemetry metrics adapter for trembita ([observability §2 export](../../docs/decisions/observability.md)).

- [`init_metrics_with_otlp`](src/lib.rs) — global OTLP/gRPC meter provider; [`OtlpMetricsSink`](src/sink.rs) bridged via [`trembita::install_otlp_metrics`](../trembita/src/app/otlp_metrics.rs).
- Enable on the facade with `features = ["otlp-metrics"]`; `TrembitaApp::from_env` wires the sink when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (B-51).
- Product metric names + alert hints: [env.md § B-51](../../docs/env.md#opentelemetry-metrics-b-51) · [runbook § B-51](../../docs/ops/production-runbook.md#ops-observability-layer-b-51).

Pull metrics via `GET /metrics` remain the default; this crate is for push/export backends only.
