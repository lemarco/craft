//! OTLP [`MetricsSink`](trembita_dashboard::MetricsSink) bridge (B-51).

use std::sync::Arc;

use trembita_dashboard::MetricsSink;
use trembita_metrics_otlp::{MetricsOpts, OtlpMetricsSink, init_metrics_with_otlp};

struct OtlpMetricsSinkBridge(Arc<OtlpMetricsSink>);

impl MetricsSink for OtlpMetricsSinkBridge {
    fn incr(&self, name: &str, help: &str, labels: &[(&str, &str)], by: f64) {
        self.0.incr(name, help, labels, by);
    }

    fn set(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        self.0.set(name, help, labels, value);
    }

    fn observe(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        self.0.observe(name, help, labels, value);
    }
}

fn otlp_endpoint(opts: &MetricsOpts) -> Option<String> {
    opts.otlp_endpoint
        .clone()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .filter(|s| !s.is_empty())
}

/// Install OTLP export and return a [`MetricsSink`] for [`super::builder::TrembitaAppBuilder::metrics_sink`].
#[must_use]
pub fn install_otlp_metrics(opts: MetricsOpts) -> Option<Arc<dyn MetricsSink>> {
    otlp_endpoint(&opts)?;
    init_metrics_with_otlp(opts);
    Some(Arc::new(OtlpMetricsSinkBridge(Arc::new(
        OtlpMetricsSink::new(),
    ))))
}
