//! A small, dependency-light Prometheus metrics registry (observability §2).
//!
//! Supports the three shapes trembita needs: monotonic **counters** (request
//! rates, restarts, leader changes), **gauges** (role, commit index, mailbox
//! depth, live nodes), and lightweight **summaries** (count + sum of a latency,
//! enough for averages without per-bucket cost). Rendering produces the
//! Prometheus text exposition format served at `GET /metrics`.
//!
//! The registry is cheap and always-on: `Metrics` is an `Arc` handle shared by
//! the runtime (which records samples) and the admin server (which renders).
//! Optional [`MetricsSink`](crate::MetricsSink) hooks receive the same samples
//! for push export without replacing Prometheus scrape.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use crate::metrics_sink::MetricsSink;

/// The Prometheus type of a metric family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricKind {
    /// Monotonically increasing value (`# TYPE … counter`).
    Counter,
    /// Instantaneous value that can go up or down (`# TYPE … gauge`).
    Gauge,
    /// Observation count + sum, rendered as `_count`/`_sum` (`… summary`).
    Summary,
}

impl MetricKind {
    fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Summary => "summary",
        }
    }
}

#[derive(Clone)]
enum Sample {
    Scalar(f64),
    Summary { count: u64, sum: f64 },
}

struct Family {
    kind: MetricKind,
    help: String,
    /// Keyed by the rendered label set (empty or `{k="v",…}`), value per series.
    series: BTreeMap<String, Sample>,
}

#[derive(Default)]
struct Registry {
    families: BTreeMap<String, Family>,
}

/// A shared, cloneable Prometheus metrics registry.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Mutex<Registry>>,
    extra_sinks: Arc<[Arc<dyn MetricsSink>]>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Format a label slice into a deterministic Prometheus label set, e.g.
/// `{role="leader",node="2"}`. Empty slice → empty string.
fn format_labels(labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<(&str, &str)> = labels.to_vec();
    sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let mut out = String::from("{");
    for (i, (k, v)) in sorted.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        // Escape per Prometheus text format (\, ", and newline).
        let escaped = v
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        let _ = write!(out, "{k}=\"{escaped}\"");
    }
    out.push('}');
    out
}

impl Metrics {
    /// A fresh, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Registry::default())),
            extra_sinks: Arc::from([]),
        }
    }

    /// Same as [`new`](Self::new) but also forwards every sample to `sinks`.
    #[must_use]
    pub fn with_extra_sinks(sinks: Vec<Arc<dyn MetricsSink>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Registry::default())),
            extra_sinks: Arc::from(sinks),
        }
    }

    fn forward<F>(&self, record: F)
    where
        F: Fn(&dyn MetricsSink),
    {
        for sink in self.extra_sinks.iter() {
            record(sink.as_ref());
        }
    }

    fn family<'a>(
        reg: &'a mut Registry,
        name: &str,
        kind: MetricKind,
        help: &str,
    ) -> &'a mut Family {
        reg.families
            .entry(name.to_owned())
            .or_insert_with(|| Family {
                kind,
                help: help.to_owned(),
                series: BTreeMap::new(),
            })
    }

    /// Add `by` to a counter series (created on first use).
    ///
    /// # Panics
    /// Panics if the registry lock is poisoned.
    pub fn incr(&self, name: &str, help: &str, labels: &[(&str, &str)], by: f64) {
        {
            let mut reg = self.inner.lock().expect("poisoned");
            let key = format_labels(labels);
            let family = Self::family(&mut reg, name, MetricKind::Counter, help);
            match family.series.entry(key).or_insert(Sample::Scalar(0.0)) {
                Sample::Scalar(v) => *v += by,
                Sample::Summary { .. } => {}
            }
        }
        self.forward(|sink| sink.incr(name, help, labels, by));
    }

    /// Set a gauge series to `value` (created on first use).
    ///
    /// # Panics
    /// Panics if the registry lock is poisoned.
    pub fn set(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        {
            let mut reg = self.inner.lock().expect("poisoned");
            let key = format_labels(labels);
            let family = Self::family(&mut reg, name, MetricKind::Gauge, help);
            family.series.insert(key, Sample::Scalar(value));
        }
        self.forward(|sink| sink.set(name, help, labels, value));
    }

    /// Record an observation into a summary series (count += 1, sum += value).
    ///
    /// # Panics
    /// Panics if the registry lock is poisoned.
    pub fn observe(&self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        {
            let mut reg = self.inner.lock().expect("poisoned");
            let key = format_labels(labels);
            let family = Self::family(&mut reg, name, MetricKind::Summary, help);
            match family
                .series
                .entry(key)
                .or_insert(Sample::Summary { count: 0, sum: 0.0 })
            {
                Sample::Summary { count, sum } => {
                    *count += 1;
                    *sum += value;
                }
                Sample::Scalar(_) => {}
            }
        }
        self.forward(|sink| sink.observe(name, help, labels, value));
    }

    /// Render the whole registry as Prometheus text exposition format.
    ///
    /// # Panics
    /// Panics if the registry lock is poisoned.
    #[must_use]
    pub fn render(&self) -> String {
        let reg = self.inner.lock().expect("poisoned");
        let mut out = String::new();
        for (name, family) in &reg.families {
            let _ = writeln!(out, "# HELP {name} {}", family.help);
            let _ = writeln!(out, "# TYPE {name} {}", family.kind.as_str());
            for (labels, sample) in &family.series {
                match sample {
                    Sample::Scalar(v) => {
                        let _ = writeln!(out, "{name}{labels} {v}");
                    }
                    Sample::Summary { count, sum } => {
                        let inner = strip_braces(labels);
                        let _ = writeln!(out, "{name}_count{{{inner}}} {count}");
                        let _ = writeln!(out, "{name}_sum{{{inner}}} {sum}");
                    }
                }
            }
        }
        out
    }
}

/// Strip the outer `{…}` from a formatted label set for splicing into a
/// summary's `_count`/`_sum` sub-metrics. Empty → empty.
fn strip_braces(labels: &str) -> &str {
    labels
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_and_render() {
        let m = Metrics::new();
        m.incr(
            "trembita_client_requests_total",
            "Client requests.",
            &[],
            1.0,
        );
        m.incr(
            "trembita_client_requests_total",
            "Client requests.",
            &[],
            2.0,
        );
        let out = m.render();
        assert!(out.contains("# TYPE trembita_client_requests_total counter"));
        assert!(out.contains("trembita_client_requests_total 3"));
    }

    #[test]
    fn gauges_overwrite_and_labels_are_sorted() {
        let m = Metrics::new();
        m.set(
            "trembita_raft_commit_index",
            "Commit index.",
            &[("node", "2")],
            5.0,
        );
        m.set(
            "trembita_raft_commit_index",
            "Commit index.",
            &[("node", "2")],
            9.0,
        );
        m.set(
            "trembita_raft_role",
            "Role.",
            &[("z", "1"), ("a", "2")],
            1.0,
        );
        let out = m.render();
        assert!(out.contains("trembita_raft_commit_index{node=\"2\"} 9"));
        // Labels rendered in sorted key order regardless of input order.
        assert!(out.contains("trembita_raft_role{a=\"2\",z=\"1\"} 1"));
    }

    #[test]
    fn summaries_emit_count_and_sum() {
        let m = Metrics::new();
        m.observe(
            "trembita_handle_latency_seconds",
            "Handle latency.",
            &[],
            0.5,
        );
        m.observe(
            "trembita_handle_latency_seconds",
            "Handle latency.",
            &[],
            1.5,
        );
        let out = m.render();
        assert!(out.contains("# TYPE trembita_handle_latency_seconds summary"));
        assert!(out.contains("trembita_handle_latency_seconds_count{} 2"));
        assert!(out.contains("trembita_handle_latency_seconds_sum{} 2"));
    }

    #[test]
    fn extra_sink_receives_prometheus_samples() {
        use crate::RecordingMetricsSink;

        let recorder = RecordingMetricsSink::new();
        let m = Metrics::with_extra_sinks(vec![Arc::new(recorder.clone())]);
        m.incr("trembita_test_total", "test", &[], 1.0);
        assert!(m.render().contains("trembita_test_total 1"));
        assert_eq!(
            recorder.take_samples(),
            vec![crate::RecordedMetric::Incr {
                name: "trembita_test_total".into(),
                help: "test".into(),
                labels: vec![],
                by: 1.0,
            }]
        );
    }
}
