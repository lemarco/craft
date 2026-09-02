//! Leader-side failure detectors for reachability (liveness-vs-membership).
//!
//! The default [`FailureDetectorKind::AckWindow`] pairs a configurable silence
//! window with hysteresis so a briefly slow follower does not flap the
//! supervisor. [`FailureDetectorKind::PhiAccrual`] implements the phi-accrual
//! detector from the Haystack paper as a documented alternative when network
//! jitter is high.

use std::collections::{BTreeMap, VecDeque};

use trembita_proto::NodeId;

/// Which algorithm derives per-peer reachability on the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailureDetectorKind {
    /// Silence longer than the reachability window marks a peer unreachable;
    /// recovery requires a fresh ack within `window − hysteresis`.
    #[default]
    AckWindow,
    /// Inter-arrival statistics; suspect when φ exceeds the configured threshold.
    PhiAccrual,
}

/// Tunable reachability parameters (logical ticks).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReachabilityConfig {
    /// Ticks of silence before a **reachable** peer is marked unreachable.
    /// `None` ⇒ `2 × election_timeout_max` at runtime.
    pub window_ticks: Option<u64>,
    /// Hysteresis band: a peer marked unreachable needs an ack within
    /// `window − hysteresis` to flip back (reduces reconcile flapping).
    pub hysteresis_ticks: u64,
    /// Detector algorithm.
    pub detector: FailureDetectorKind,
    /// φ threshold when [`FailureDetectorKind::PhiAccrual`] is active (typical 8–12).
    pub phi_threshold: f64,
}

impl Default for ReachabilityConfig {
    fn default() -> Self {
        Self {
            window_ticks: None,
            hysteresis_ticks: 0,
            detector: FailureDetectorKind::AckWindow,
            phi_threshold: 8.0,
        }
    }
}

impl ReachabilityConfig {
    /// Resolve the silence window from config and election timing.
    #[must_use]
    pub fn window(&self, election_timeout_max: u64) -> u64 {
        self.window_ticks
            .unwrap_or_else(|| election_timeout_max.saturating_mul(2))
    }

    /// Hysteresis band; when zero, callers fall back to `election_timeout_min`.
    #[must_use]
    pub fn hysteresis(&self, election_timeout_min: u64) -> u64 {
        if self.hysteresis_ticks > 0 {
            self.hysteresis_ticks
        } else {
            election_timeout_min
        }
    }
}

/// Per-peer latched reachability with ack-window + hysteresis.
#[derive(Debug, Clone, Default)]
pub struct AckWindowLiveness {
    latched: BTreeMap<NodeId, bool>,
}

impl AckWindowLiveness {
    /// Recompute latched reachability for every voter except `self_id`.
    pub fn update(
        &mut self,
        now: u64,
        self_id: NodeId,
        voters: &[NodeId],
        last_ack: &BTreeMap<NodeId, u64>,
        window: u64,
        hysteresis: u64,
    ) {
        let high = window;
        let low = window.saturating_sub(hysteresis);
        for &peer in voters {
            if peer == self_id {
                continue;
            }
            let silence = last_ack
                .get(&peer)
                .map_or(u64::MAX, |&t| now.saturating_sub(t));
            let entry = self.latched.entry(peer).or_insert(true);
            if *entry {
                if silence > high {
                    *entry = false;
                }
            } else if silence <= low {
                *entry = true;
            }
        }
    }

    /// Whether `peer` is currently considered reachable (defaults to true).
    #[must_use]
    pub fn is_reachable(&self, peer: NodeId) -> bool {
        self.latched.get(&peer).copied().unwrap_or(true)
    }

    /// Clear state on leadership change.
    pub fn clear(&mut self) {
        self.latched.clear();
    }
}

/// Phi-accrual failure detector for one peer (Haystack-style, tick time base).
#[derive(Debug, Clone)]
pub struct PhiAccrualDetector {
    history: VecDeque<u64>,
    last_heartbeat: Option<u64>,
    threshold: f64,
    max_samples: usize,
}

impl PhiAccrualDetector {
    /// Create a detector with the given φ suspect threshold.
    #[must_use]
    pub fn new(threshold: f64) -> Self {
        Self {
            history: VecDeque::new(),
            last_heartbeat: None,
            threshold,
            max_samples: 1000,
        }
    }

    /// Record a successful heartbeat ack at logical tick `now`.
    pub fn record_heartbeat(&mut self, now: u64) {
        if let Some(last) = self.last_heartbeat {
            let interval = now.saturating_sub(last);
            if interval > 0 {
                if self.history.len() >= self.max_samples {
                    self.history.pop_front();
                }
                self.history.push_back(interval);
            }
        }
        self.last_heartbeat = Some(now);
    }

    /// φ value: higher ⇒ more likely the peer is down.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // heartbeat intervals are small; f64 stats are intentional
    pub fn phi(&self, now: u64) -> f64 {
        let Some(last) = self.last_heartbeat else {
            return 0.0;
        };
        let time_since = now.saturating_sub(last) as f64;
        if self.history.is_empty() {
            // No samples yet — use a conservative pause before suspecting.
            return if time_since > 100.0 {
                self.threshold + 1.0
            } else {
                0.0
            };
        }
        let n = self.history.len() as f64;
        let mean = self.history.iter().map(|&x| x as f64).sum::<f64>() / n;
        let variance = self
            .history
            .iter()
            .map(|&x| {
                let d = x as f64 - mean;
                d * d
            })
            .sum::<f64>()
            / n;
        let std_dev = variance.sqrt().max(1.0);
        let y = (time_since - mean) / std_dev;
        let p = 1.0 - normal_cdf(y);
        if p <= f64::MIN_POSITIVE {
            return f64::MAX;
        }
        (-p.log10()).max(0.0)
    }

    /// Whether the peer is considered alive at `now`.
    #[must_use]
    pub fn is_available(&self, now: u64) -> bool {
        self.phi(now) < self.threshold
    }
}

/// Per-peer phi detectors for all voters.
#[derive(Debug, Clone, Default)]
pub struct PhiAccrualLiveness {
    detectors: BTreeMap<NodeId, PhiAccrualDetector>,
    threshold: f64,
}

impl PhiAccrualLiveness {
    /// Create a bank of detectors sharing `threshold`.
    #[must_use]
    pub fn new(threshold: f64) -> Self {
        Self {
            detectors: BTreeMap::new(),
            threshold,
        }
    }

    /// Record an ack for `peer`.
    pub fn record_heartbeat(&mut self, peer: NodeId, now: u64) {
        self.detectors
            .entry(peer)
            .or_insert_with(|| PhiAccrualDetector::new(self.threshold))
            .record_heartbeat(now);
    }

    /// Whether `peer` is reachable under phi-accrual.
    #[must_use]
    pub fn is_reachable(&self, peer: NodeId, now: u64) -> bool {
        self.detectors
            .get(&peer)
            .is_none_or(|d| d.is_available(now))
    }

    /// Drop all per-peer phi-accrual state.
    pub fn clear(&mut self) {
        self.detectors.clear();
    }
}

/// Standard-normal CDF approximation (Abramowitz & Stegun 26.2.17).
fn normal_cdf(x: f64) -> f64 {
    if x.is_nan() {
        return 0.5;
    }
    let t = 1.0 / (1.0 + 0.231_641_9 * x.abs());
    let poly = t
        * (0.319_381_530
            + t * (-0.356_563_782
                + t * (1.781_477_937 + t * (-1.821_255_978 + t * 1.330_274_429))));
    let pdf = (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let p = 1.0 - pdf * poly;
    if x < 0.0 { 1.0 - p } else { p }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hysteresis_prevents_immediate_flap_back() {
        let mut live = AckWindowLiveness::default();
        let voters = [NodeId(1), NodeId(2), NodeId(3)];
        let mut acks = BTreeMap::new();
        acks.insert(NodeId(2), 0);
        acks.insert(NodeId(3), 0);

        // Peer 2 silent past the high watermark → unreachable.
        live.update(50, NodeId(1), &voters, &acks, 40, 10);
        assert!(!live.is_reachable(NodeId(2)));

        // Fresh ack within low band (30 ticks) → reachable again.
        acks.insert(NodeId(2), 25);
        live.update(30, NodeId(1), &voters, &acks, 40, 10);
        assert!(live.is_reachable(NodeId(2)));
    }

    #[test]
    fn phi_rises_when_heartbeats_stop() {
        let mut det = PhiAccrualDetector::new(8.0);
        for t in (1..=20).map(|i| i * 5) {
            det.record_heartbeat(t);
        }
        assert!(det.is_available(100));
        assert!(!det.is_available(500));
        assert!(det.phi(500) > det.phi(100));
    }
}
