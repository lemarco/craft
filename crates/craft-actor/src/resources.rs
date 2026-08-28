//! VPS resource profile for the one-worker-per-VPS model (backlog E13,
//! [cluster-elasticity#one-worker-per-vps-production](../../../docs/decisions/cluster-elasticity.md#one-worker-per-vps-production)).
//!
//! In production `craft` runs a **single** worker actor per VPS and expects
//! that worker to use the whole machine — parallelism lives *inside* the actor
//! (internal thread pools, batching, async concurrency), not across many actor
//! instances on one node. [`VpsResources`] tells the worker how much capacity it
//! has so it can size those internals; [`ResourceProfile`] chooses between using
//! the whole VPS (production default) and a fixed cap (dev / tests).

use std::num::NonZeroUsize;

/// How much of a VPS the single worker should use (one-worker-per-vps).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResourceProfile {
    /// Production default: expose the full VPS capacity to the one worker.
    #[default]
    UseAllAvailable,
    /// Cap the worker to `worker_threads` (development / tests / shared hosts).
    Limited {
        /// The worker-thread budget the actor should size itself to (min 1).
        worker_threads: usize,
    },
}

/// The VPS capacity handed to the single worker so it can size its internal
/// pools (one-worker-per-vps). Cheap value object; derived from a [`ResourceProfile`] and
/// the machine's detected parallelism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VpsResources {
    /// Detected hardware parallelism ([`std::thread::available_parallelism`]).
    pub available_parallelism: usize,
    /// Worker-thread count the runtime is sized to (matches
    /// `available_parallelism` under [`ResourceProfile::UseAllAvailable`]).
    pub tokio_worker_threads: usize,
    /// A suggested size for the actor's own internal work pool.
    pub suggested_internal_pool: usize,
}

impl VpsResources {
    /// Detect the machine's parallelism and apply `profile`.
    ///
    /// Falls back to a parallelism of `1` if detection is unavailable.
    #[must_use]
    pub fn detect(profile: ResourceProfile) -> Self {
        let available = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        Self::from_parallelism(available, profile)
    }

    /// Build resources for a known `available` parallelism and `profile`.
    /// Deterministic — used in tests and when parallelism is configured
    /// explicitly. `available` is treated as at least `1`.
    #[must_use]
    pub fn from_parallelism(available: usize, profile: ResourceProfile) -> Self {
        let available = available.max(1);
        let threads = match profile {
            ResourceProfile::UseAllAvailable => available,
            // An explicit cap is honored even if it exceeds detected cores (a
            // dev box may over-subscribe to simulate a larger machine).
            ResourceProfile::Limited { worker_threads } => worker_threads.max(1),
        };
        Self {
            available_parallelism: available,
            tokio_worker_threads: threads,
            suggested_internal_pool: threads,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_uses_all_available() {
        assert_eq!(ResourceProfile::default(), ResourceProfile::UseAllAvailable);
    }

    #[test]
    fn use_all_available_matches_detected_parallelism() {
        let r = VpsResources::from_parallelism(8, ResourceProfile::UseAllAvailable);
        assert_eq!(r.available_parallelism, 8);
        assert_eq!(r.tokio_worker_threads, 8);
        assert_eq!(r.suggested_internal_pool, 8);
    }

    #[test]
    fn limited_profile_caps_worker_threads() {
        let r = VpsResources::from_parallelism(16, ResourceProfile::Limited { worker_threads: 4 });
        assert_eq!(r.available_parallelism, 16, "still reports real hardware");
        assert_eq!(r.tokio_worker_threads, 4, "capped to the requested budget");
    }

    #[test]
    fn parallelism_and_threads_are_never_zero() {
        let r = VpsResources::from_parallelism(0, ResourceProfile::Limited { worker_threads: 0 });
        assert_eq!(r.available_parallelism, 1);
        assert_eq!(r.tokio_worker_threads, 1);
    }

    #[test]
    fn detect_reports_a_sane_machine() {
        let r = VpsResources::detect(ResourceProfile::UseAllAvailable);
        assert!(r.available_parallelism >= 1);
        assert_eq!(r.tokio_worker_threads, r.available_parallelism);
    }
}
