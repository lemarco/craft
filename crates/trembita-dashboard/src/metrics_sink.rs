//! Optional metrics export port (observability §2 export).
//!
//! [`Metrics`] always maintains an in-process Prometheus registry for
//! `GET /metrics`. Implement [`MetricsSink`] to fan out the same samples to an
//! external backend (StatsD, OTLP adapter crate, custom logging, etc.) without
//! pulling OpenTelemetry into the default dependency tree.

use std::sync::{Arc, Mutex};

/// Receives the same counter, gauge, and summary samples recorded by the runtime.
pub trait MetricsSink: Send + Sync {
    /// Add `by` to a counter series.
    fn incr(&self, name: &str, help: &str, labels: &[(&str, &str)], by: f64);
    /// Set a gauge series.
    fn set(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64);
    /// Record one summary observation.
    fn observe(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64);
}

/// A sink that discards all samples (tests and placeholders).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetricsSink;

impl MetricsSink for NoopMetricsSink {
    fn incr(&self, _name: &str, _help: &str, _labels: &[(&str, &str)], _by: f64) {}

    fn set(&self, _name: &str, _help: &str, _labels: &[(&str, &str)], _value: f64) {}

    fn observe(&self, _name: &str, _help: &str, _labels: &[(&str, &str)], _value: f64) {}
}

/// Forwards each sample to every wrapped sink.
#[derive(Clone, Default)]
pub struct MultiMetricsSink {
    sinks: Arc<[Arc<dyn MetricsSink>]>,
}

impl MultiMetricsSink {
    /// Build a fan-out sink. An empty slice is allowed (equivalent to [`NoopMetricsSink`]).
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn MetricsSink>>) -> Arc<Self> {
        Arc::new(Self {
            sinks: sinks.into(),
        })
    }
}

impl MetricsSink for MultiMetricsSink {
    fn incr(&self, name: &str, help: &str, labels: &[(&str, &str)], by: f64) {
        for sink in self.sinks.iter() {
            sink.incr(name, help, labels, by);
        }
    }

    fn set(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        for sink in self.sinks.iter() {
            sink.set(name, help, labels, value);
        }
    }

    fn observe(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        for sink in self.sinks.iter() {
            sink.observe(name, help, labels, value);
        }
    }
}

/// One recorded sample (for tests and custom adapters).
#[derive(Debug, Clone, PartialEq)]
pub enum RecordedMetric {
    /// Counter increment.
    Incr {
        /// Metric name.
        name: String,
        /// HELP text.
        help: String,
        /// Label set.
        labels: Vec<(String, String)>,
        /// Delta.
        by: f64,
    },
    /// Gauge set.
    Set {
        /// Metric name.
        name: String,
        /// HELP text.
        help: String,
        /// Label set.
        labels: Vec<(String, String)>,
        /// Value.
        value: f64,
    },
    /// Summary observation.
    Observe {
        /// Metric name.
        name: String,
        /// HELP text.
        help: String,
        /// Label set.
        labels: Vec<(String, String)>,
        /// Observed value.
        value: f64,
    },
}

fn labels_owned(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Captures samples in memory (integration tests and sink prototyping).
#[derive(Clone, Default)]
pub struct RecordingMetricsSink {
    samples: Arc<Mutex<Vec<RecordedMetric>>>,
}

impl RecordingMetricsSink {
    /// Fresh, empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot and clear captured samples.
    ///
    /// # Panics
    /// Panics if the recorder lock is poisoned.
    #[must_use]
    pub fn take_samples(&self) -> Vec<RecordedMetric> {
        std::mem::take(&mut *self.samples.lock().expect("poisoned"))
    }
}

impl MetricsSink for RecordingMetricsSink {
    fn incr(&self, name: &str, help: &str, labels: &[(&str, &str)], by: f64) {
        self.samples
            .lock()
            .expect("poisoned")
            .push(RecordedMetric::Incr {
                name: name.to_owned(),
                help: help.to_owned(),
                labels: labels_owned(labels),
                by,
            });
    }

    fn set(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        self.samples
            .lock()
            .expect("poisoned")
            .push(RecordedMetric::Set {
                name: name.to_owned(),
                help: help.to_owned(),
                labels: labels_owned(labels),
                value,
            });
    }

    fn observe(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        self.samples
            .lock()
            .expect("poisoned")
            .push(RecordedMetric::Observe {
                name: name.to_owned(),
                help: help.to_owned(),
                labels: labels_owned(labels),
                value,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_sink_fans_out_to_all_children() {
        let a = RecordingMetricsSink::new();
        let b = RecordingMetricsSink::new();
        let multi = MultiMetricsSink::new(vec![Arc::new(a.clone()), Arc::new(b.clone())]);
        multi.incr("trembita_test_total", "test", &[("node", "1")], 2.0);
        multi.set("trembita_test_gauge", "test", &[], 3.0);
        multi.observe("trembita_test_latency_seconds", "test", &[], 0.25);

        let expected = vec![
            RecordedMetric::Incr {
                name: "trembita_test_total".into(),
                help: "test".into(),
                labels: vec![("node".into(), "1".into())],
                by: 2.0,
            },
            RecordedMetric::Set {
                name: "trembita_test_gauge".into(),
                help: "test".into(),
                labels: vec![],
                value: 3.0,
            },
            RecordedMetric::Observe {
                name: "trembita_test_latency_seconds".into(),
                help: "test".into(),
                labels: vec![],
                value: 0.25,
            },
        ];
        assert_eq!(a.take_samples(), expected);
        assert_eq!(b.take_samples(), expected);
    }
}
