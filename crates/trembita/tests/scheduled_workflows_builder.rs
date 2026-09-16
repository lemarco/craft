//! [`.scheduled_workflows`](trembita::TrembitaAppBuilder::scheduled_workflows) boot validation.

use std::time::Duration;

use trembita::{RunOpts, ScheduledWorkflowOpts, TrembitaApp};

#[tokio::test]
async fn scheduled_workflows_empty_fails_at_boot() {
    let result = TrembitaApp::builder()
        .data_dir(tempfile::tempdir().expect("tempdir").path())
        .scheduled_workflows(ScheduledWorkflowOpts::new())
        .boot_for_test(RunOpts::local())
        .await;
    let err = result.err().expect("expected boot failure");
    assert!(err.to_string().contains("scheduled_workflows"));
}

#[tokio::test]
async fn scheduled_workflows_registers_stream() {
    TrembitaApp::builder()
        .data_dir(tempfile::tempdir().expect("tempdir").path())
        .scheduled_workflows(
            ScheduledWorkflowOpts::new()
                .lease(Duration::from_secs(60))
                .workflow("weekly", "0 3 * * 1", "weekly-report"),
        )
        .boot_for_test(RunOpts::local())
        .await
        .expect("boot");
}
