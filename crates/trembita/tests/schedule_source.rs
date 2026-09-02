//! [`ScheduleSource`] wiring on [`TrembitaApp`].

use std::sync::Arc;
use std::time::Duration;

use trembita::{QueueOpts, SchedulePoll, TrembitaApp, TrembitaConfigure};
use trembita_actor::{RecurringJob, StaticScheduleSource};
use trembita_test_support::boot_local_app;

#[tokio::test]
async fn schedule_source_wires_on_app_boot() {
    let base = std::env::temp_dir().join(format!(
        "trembita-sched-boot-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let _app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new("jobs", Duration::from_secs(30))])
                .schedule_source(
                    "jobs",
                    Arc::new(StaticScheduleSource::new(vec![RecurringJob::new(
                        "daily",
                        "0 9 * * *",
                        b"tick",
                    )])),
                    SchedulePoll::secs(1),
                )
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;
}
