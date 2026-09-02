//! [`CraftyApp`] smoke tests (B-02).

use std::time::Duration;

use crafty::{CraftyApp, CraftyConfigure, QueueOpts};
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_app_leader};

#[tokio::test(start_paused = true)]
async fn crafty_app_start_local_with_data_dir_and_queue() {
    let base = std::env::temp_dir().join(format!(
        "crafty-app-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            CraftyApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .configure(CraftyConfigure {
                    tick_period: Duration::from_millis(5),
                    ..CraftyConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_crafty_app_leader(&app).await;
    // Let the keepalive loop refresh [`ClusterFacts`] before queue RPC routing.
    advance(Duration::from_millis(200)).await;

    assert!(app.actor_state_store().is_some());
    let id = app.enqueue("jobs", b"hello").await.expect("enqueue");
    assert!(id.0 >= 1);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn crafty_app_requeue_dead_letter() {
    use crafty::actor::WorkerId;
    use crafty::cluster::EnqueueOptions;
    use crafty_actor::JobLifecycle;

    let base = std::env::temp_dir().join(format!(
        "crafty-app-requeue-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            CraftyApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .configure(CraftyConfigure {
                    tick_period: Duration::from_millis(5),
                    ..CraftyConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_crafty_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let id = app
        .enqueue_opts("jobs", b"poison", EnqueueOptions::max_attempts(1))
        .await
        .expect("enqueue");
    let queue = app.job_queue("jobs").expect("queue");
    let worker = WorkerId {
        node: app.node_id(),
        instance: 0,
    };
    let leased = queue.lease(worker, 1).await.expect("lease");
    queue.nack(worker, leased[0].lease_id).await.expect("nack");
    advance(Duration::from_secs(2)).await;

    let status = app
        .job_status("jobs", id)
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.lifecycle, JobLifecycle::DeadLetter);

    app.requeue_dead_letter("jobs", id).await.expect("requeue");
    let pending = app
        .job_status("jobs", id)
        .await
        .expect("status")
        .expect("row");
    assert_eq!(pending.lifecycle, JobLifecycle::Pending);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
