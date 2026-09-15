//! Optional OpenTelemetry metrics export (observability backlog O-03).

use std::sync::OnceLock;

static INIT: OnceLock<()> = OnceLock::new();

/// Options for [`init_metrics_with_otlp`].
#[derive(Debug, Clone)]
pub struct MetricsOpts {
    /// `service.name` resource attribute.
    pub service_name: String,
    /// When set, export metrics via OTLP/gRPC (`OTEL_EXPORTER_OTLP_ENDPOINT` overrides).
    pub otlp_endpoint: Option<String>,
}

impl MetricsOpts {
    /// Sensible defaults for a named service.
    #[must_use]
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            otlp_endpoint: None,
        }
    }

    /// Read `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_SERVICE_NAME` when present.
    #[must_use]
    pub fn from_env(service_name: impl Into<String>) -> Self {
        let mut opts = Self::new(service_name);
        opts.otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
        if let Ok(name) = std::env::var("OTEL_SERVICE_NAME") {
            opts.service_name = name;
        }
        opts
    }
}

/// Install a global OTLP metrics pipeline. Idempotent; no-op when endpoint is unset.
///
/// Pair with `trembita_runtime::init_tracing_with_otlp` when exporting traces and metrics.
pub fn init_metrics_with_otlp(opts: MetricsOpts) {
    INIT.get_or_init(|| install(opts));
}

fn install(opts: MetricsOpts) {
    let endpoint = opts
        .otlp_endpoint
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok());
    let Some(endpoint) = endpoint else {
        tracing::debug!(service = %opts.service_name, "metrics: no OTLP endpoint; skipped");
        return;
    };
    match install_otlp(&opts.service_name, &endpoint) {
        Ok(()) => tracing::info!(
            service = %opts.service_name,
            endpoint = %endpoint,
            "metrics initialized with OTLP"
        ),
        Err(err) => tracing::warn!(service = %opts.service_name, %err, "metrics OTLP init failed"),
    }
}

fn install_otlp(service_name: &str, endpoint: &str) -> Result<(), String> {
    use opentelemetry::KeyValue;
    use opentelemetry::global;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| format!("otlp metrics exporter: {e}"))?;

    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", service_name.to_string())])
                .build(),
        )
        .build();

    global::set_meter_provider(provider);
    Ok(())
}
