//! Event topic facade integration: publish, subscriptions, retention.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use trembita::{TopicOpts, TrembitaApp, TrembitaConfigure};
use trembita_events::{SubscriptionStart, TopicSubscriptionDef};
use trembita_test_support::boot_local_app;

#[tokio::test]
async fn trembita_app_publishes_and_tracks_topic_metrics() {
    let base = std::env::temp_dir().join(format!(
        "trembita-topic-facade-{}",
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
                .topics([TopicOpts::topic("orders.events").subscription_defs([
                    TopicSubscriptionDef {
                        name: "analytics".into(),
                        start: SubscriptionStart::Earliest,
                        max_attempts: 0,
                    },
                ])])
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

    app.publish("orders.events", b"order-created")
        .await
        .expect("publish");

    let topic = app.event_topic("orders.events").unwrap();
    for _ in 0..200 {
        let metrics = topic.metrics().await.unwrap();
        if metrics.event_count >= 1 {
            assert_eq!(metrics.event_count, 1);
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("topic metrics never showed published event");
}
