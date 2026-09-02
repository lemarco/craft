//! External compute load port — subprocess / shell-out pressure the token pool
//! cannot observe ([external-load](../../../docs/decisions/external-load.md)).

use std::sync::atomic::{AtomicUsize, Ordering};

/// Process-external compute pressure reported by the application.
///
/// The workload governor maps [`Self::units`] into ingress pressure so API
/// protection kicks in when child processes (Chromium, ffmpeg, …) compete with
/// the gateway even though [`crate::ComputeTokenPool`] holders may be idle on IO.
pub trait ExternalLoad: Send + Sync {
    /// Extra compute units consumed outside the cooperative token pool (minimum 0).
    fn units(&self) -> usize;
}

/// Manual external load counter for tests and simple integrations.
#[derive(Debug, Default)]
pub struct ManualExternalLoad {
    units: AtomicUsize,
}

impl ManualExternalLoad {
    /// Zero external load.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the reported load.
    pub fn set(&self, units: usize) {
        self.units.store(units, Ordering::Release);
    }
}

impl ExternalLoad for ManualExternalLoad {
    fn units(&self) -> usize {
        self.units.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_external_load_tracks_units() {
        let load = ManualExternalLoad::new();
        assert_eq!(load.units(), 0);
        load.set(3);
        assert_eq!(load.units(), 3);
    }
}
