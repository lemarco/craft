//! [`CraftyApp`] smoke tests (B-02).

use std::time::Duration;

use crafty::net::LocalNetwork;
use crafty::{CraftyApp, NodeId};
use crafty_test_support::{advance, wait_for_crafty_leader};

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

    let net = LocalNetwork::new();
    let app = CraftyApp::builder(NodeId(1))
        .data_dir(&base)
        .job_stream("jobs", Duration::from_secs(60))
        .members([NodeId(1)])
        .tick_period(Duration::from_millis(5))
        .start_local(&net)
        .await;

    wait_for_crafty_leader(app.cluster()).await;
    // Let the keepalive loop refresh [`ClusterFacts`] before queue RPC routing.
    advance(Duration::from_millis(200)).await;

    assert!(app.actor_state_store().is_some());
    let id = app.enqueue("jobs", b"hello").await.expect("enqueue");
    assert!(id.0 >= 1);

    app.cluster().shutdown();
    let _ = std::fs::remove_dir_all(base);
}
