//! Cron → workflow registration for the showcase manifest.

use trembita::ScheduledWorkflowOpts;

/// Weekly cron that starts the `weekly-report` saga (ledger capability group).
#[must_use]
pub fn weekly_scheduled_workflows() -> ScheduledWorkflowOpts {
    ScheduledWorkflowOpts::new().workflow("weekly", "0 3 * * 1", "weekly-report")
}
