//! Shared retry / dead-letter outcome after a failed lease attempt.

/// Result of [`after_failed_attempt`] — shared by job queue and event topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptOutcome {
    /// Attempt count after this failure.
    pub attempts: u32,
    /// Whether the item should move to dead-letter.
    pub dead_letter: bool,
    /// Earliest retry time (unix ms).
    pub not_before_ms: u64,
}

/// Compute retry timing after a failed lease attempt.
#[must_use]
pub fn after_failed_attempt(attempts: u32, max_attempts: u32, now_ms: u64) -> AttemptOutcome {
    let attempts = attempts.saturating_add(1);
    if max_attempts > 0 && attempts >= max_attempts {
        AttemptOutcome {
            attempts,
            dead_letter: true,
            not_before_ms: now_ms,
        }
    } else {
        let delay_ms = (1000u64 * u64::from(attempts)).min(300_000);
        AttemptOutcome {
            attempts,
            dead_letter: false,
            not_before_ms: now_ms.saturating_add(delay_ms),
        }
    }
}
