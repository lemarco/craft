//! Directory delivery counters for R3 visibility (B-36).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type NoTargetHook = Arc<dyn Fn(&str) + Send + Sync>;

/// Cumulative `NoTarget` counts from [`ClusterMessaging`](crate::ClusterMessaging) resolves.
#[derive(Default)]
pub struct DirectoryDeliveryStats {
    no_target: Mutex<BTreeMap<String, u64>>,
    on_no_target: Mutex<Option<NoTargetHook>>,
}

impl DirectoryDeliveryStats {
    /// Record one directory miss for `group` (cast/ask/session resolve).
    pub fn record_no_target(&self, group: &str) {
        {
            let mut counts = self.no_target.lock().expect("poisoned");
            *counts.entry(group.to_string()).or_insert(0) += 1;
        }
        if let Some(hook) = self.on_no_target.lock().expect("poisoned").clone() {
            hook(group);
        }
    }

    /// Optional hook for telemetry (installed by assembly).
    pub fn set_on_no_target(&self, hook: NoTargetHook) {
        *self.on_no_target.lock().expect("poisoned") = Some(hook);
    }

    /// Point-in-time copy of per-group `NoTarget` totals.
    #[must_use]
    pub fn no_target_totals(&self) -> BTreeMap<String, u64> {
        self.no_target.lock().expect("poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[test]
    fn b36_no_target_totals_accumulate_per_group() {
        let stats = DirectoryDeliveryStats::default();
        stats.record_no_target("workers");
        stats.record_no_target("workers");
        stats.record_no_target("caps");
        let totals = stats.no_target_totals();
        assert_eq!(totals.get("workers"), Some(&2));
        assert_eq!(totals.get("caps"), Some(&1));
    }

    #[test]
    fn b36_on_no_target_hook_invoked_per_record() {
        let stats = DirectoryDeliveryStats::default();
        let hits = Arc::new(AtomicU32::new(0));
        let hits_cb = Arc::clone(&hits);
        stats.set_on_no_target(Arc::new(move |group| {
            assert_eq!(group, "orders");
            hits_cb.fetch_add(1, Ordering::SeqCst);
        }));
        stats.record_no_target("orders");
        stats.record_no_target("orders");
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }
}
