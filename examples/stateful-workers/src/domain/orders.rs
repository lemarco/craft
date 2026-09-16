//! Order processing rules (pure types + decisions).

/// Outcome of attempting to process an order id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// First-time processing should run side effects.
    Applied,
    /// Same order id was already handled.
    AlreadyProcessed,
}

/// Decide whether to run side effects for `order_id`.
#[must_use]
pub fn process_order(order_id: u64, already_done: bool) -> ProcessOutcome {
    if already_done {
        ProcessOutcome::AlreadyProcessed
    } else {
        let _ = order_id;
        ProcessOutcome::Applied
    }
}
