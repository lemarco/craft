//! Optional OpenTelemetry export for product binaries.

use std::sync::OnceLock;

static INIT: OnceLock<()> = OnceLock::new();

/// Options for [`init_tracing_with_otlp`].
#[derive(Debug, Clone)]
pub struct TracingOpts {
    /// `service.name` resource attribute and tracer name.
    pub service_name: String,
    /// Default filter when `RUST_LOG` / `TREMBITA_LOG` are unset.
    pub default_filter: String,
    /// When set, export spans via OTLP/gRPC (`OTEL_EXPORTER_OTLP_ENDPOINT` overrides).
    pub otlp_endpoint: Option<String>,
}

impl TracingOpts {
    /// Sensible defaults for a named service.
    #[must_use]
    pub fn new(service_name: impl Into<String>) -> Self {
        let service_name = service_name.into();
        Self {
            default_filter: format!("info,{service_name}=debug"),
            service_name,
            otlp_endpoint: None,
        }
    }

    /// Read `OTEL_EXPORTER_OTLP_ENDPOINT` when present.
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

/// Install fmt subscriber, optionally layering OTLP export.
///
/// Idempotent like [`super::init_tracing`]. Falls back to fmt-only when OTLP init fails.
pub fn init_tracing_with_otlp(opts: TracingOpts) {
    INIT.get_or_init(|| install(opts));
}

fn install(opts: TracingOpts) {
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&opts.default_filter))
        .unwrap_or_else(|_| EnvFilter::new("warn"));

    let endpoint = opts
        .otlp_endpoint
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok());

    let otel_layer =
        endpoint
            .as_deref()
            .and_then(|ep| match install_otlp(&opts.service_name, ep) {
                Ok(tracer) => Some(tracing_opentelemetry::layer().with_tracer(tracer)),
                Err(err) => {
                    eprintln!("telemetry: OTLP init failed ({err}); fmt-only");
                    None
                }
            });

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(true));

    if let Some(layer) = otel_layer {
        registry.with(layer).init();
        tracing::info!(
            service = %opts.service_name,
            endpoint = %endpoint.as_deref().unwrap_or(""),
            "tracing initialized with OTLP"
        );
    } else {
        registry.init();
        tracing::info!(service = %opts.service_name, "tracing initialized (fmt only)");
    }
}

fn install_otlp(
    service_name: &str,
    endpoint: &str,
) -> Result<opentelemetry_sdk::trace::Tracer, String> {
    use opentelemetry::KeyValue;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| format!("otlp exporter: {e}"))?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", service_name.to_string())])
                .build(),
        )
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(provider.tracer(service_name.to_string()))
}
