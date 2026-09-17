//! OpenTelemetry instruments for runtime metric samples (B-51).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

/// Forwards counter/gauge/histogram samples to the global OTLP meter provider.
#[derive(Clone)]
pub struct OtlpMetricsSink {
    meter: Meter,
    counters: Arc<Mutex<HashMap<String, Counter<f64>>>>,
    gauges: Arc<Mutex<HashMap<String, Gauge<f64>>>>,
    histograms: Arc<Mutex<HashMap<String, Histogram<f64>>>>,
}

impl OtlpMetricsSink {
    /// Build a sink using the installed global meter provider.
    #[must_use]
    pub fn new() -> Self {
        let provider = global::meter_provider();
        Self {
            meter: provider.meter("trembita"),
            counters: Arc::new(Mutex::new(HashMap::new())),
            gauges: Arc::new(Mutex::new(HashMap::new())),
            histograms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn attrs(labels: &[(&str, &str)]) -> Vec<KeyValue> {
        labels
            .iter()
            .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
            .collect()
    }

    fn counter(&self, name: &str, help: &str) -> Counter<f64> {
        let mut guard = self.counters.lock().expect("lock");
        if let Some(c) = guard.get(name) {
            return c.clone();
        }
        let c = self
            .meter
            .f64_counter(name.to_string())
            .with_description(help.to_string())
            .build();
        guard.insert(name.to_string(), c.clone());
        c
    }

    fn gauge(&self, name: &str, help: &str) -> Gauge<f64> {
        let mut guard = self.gauges.lock().expect("lock");
        if let Some(g) = guard.get(name) {
            return g.clone();
        }
        let g = self
            .meter
            .f64_gauge(name.to_string())
            .with_description(help.to_string())
            .build();
        guard.insert(name.to_string(), g.clone());
        g
    }

    fn histogram(&self, name: &str, help: &str) -> Histogram<f64> {
        let mut guard = self.histograms.lock().expect("lock");
        if let Some(h) = guard.get(name) {
            return h.clone();
        }
        let h = self
            .meter
            .f64_histogram(name.to_string())
            .with_description(help.to_string())
            .build();
        guard.insert(name.to_string(), h.clone());
        h
    }

    /// Counter increment.
    pub fn incr(&self, name: &str, help: &str, labels: &[(&str, &str)], by: f64) {
        let attrs = Self::attrs(labels);
        self.counter(name, help).add(by, &attrs);
    }

    /// Gauge set.
    pub fn set(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        let attrs = Self::attrs(labels);
        self.gauge(name, help).record(value, &attrs);
    }

    /// Histogram observation.
    pub fn observe(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        let attrs = Self::attrs(labels);
        self.histogram(name, help).record(value, &attrs);
    }
}

impl Default for OtlpMetricsSink {
    fn default() -> Self {
        Self::new()
    }
}
