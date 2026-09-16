//! Cron → workflow in one builder line ([`ScheduledWorkflowOpts`]).
//!
//! See [cookbook-async-work](../../../docs/scenarios/cookbook-async-work.md).

use trembita::{ScheduledWorkflowOpts, TrembitaAppBuilder};

/// Register weekly cron that starts the `weekly-report` saga.
#[must_use]
pub fn register_weekly_cron(builder: TrembitaAppBuilder) -> TrembitaAppBuilder {
    builder.scheduled_workflows(
        ScheduledWorkflowOpts::new().workflow("weekly", "0 3 * * 1", "weekly-report"),
    )
}
