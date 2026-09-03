//! Event outbox drainer: leader polls source → publishes to topic.

use std::sync::Arc;
use std::time::Duration;

use trembita::{
    EventOutboxDrainOpts, EventOutboxSource, InMemoryEventOutboxSource, TopicOpts, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_support::boot_local_app;

#[tokio::test]
async fn event_outbox_drainer_publishes_to_topic() {
    let source = Arc::new(InMemoryEventOutboxSource::new());
    source.push(b"1", b"domain-fact");

    let base = std::env::temp_dir().join(format!(
        "trembita-event-outbox-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .topics([TopicOpts::topic("platform.events").outbox(
                    Arc::clone(&source) as Arc<dyn EventOutboxSource>,
                    EventOutboxDrainOpts::default().poll(Duration::from_millis(20)),
                )])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    for _ in 0..200 {
        if app.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(app.is_leader().await);

    for _ in 0..200 {
        if source.is_published(b"1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(source.is_published(b"1"));
    let topic = app.event_topic("platform.events").unwrap();
    let metrics = topic.metrics().await.unwrap();
    assert_eq!(metrics.event_count, 1);
}
