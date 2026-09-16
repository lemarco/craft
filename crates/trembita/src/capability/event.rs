//! Topic subscription bridge — event payload → inline capability ask.

use std::sync::Arc;
use std::time::Duration;

use trembita_events::run_topic_subscriber;
use trembita_jobs::WorkerId;
use trembita_proto::{self as proto, encode};

use super::wire::CapWire;
use crate::TrembitaApp;

/// Spawn the topic→ask bridge for a capability group subscription.
pub(crate) fn spawn_bridge(
    app: Arc<TrembitaApp>,
    group: String,
    topic: &'static str,
    subscription: &'static str,
    stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let topic_handle = app
            .event_topic(topic)
            .expect("capability event topic must be registered");
        let worker = WorkerId {
            node: app.node_id(),
            instance: 0,
        };
        run_topic_subscriber(
            topic_handle,
            topic,
            subscription,
            worker,
            1,
            Duration::from_millis(10),
            stop,
            move |event| {
                let app = Arc::clone(&app);
                let group = group.clone();
                let payload = event.payload.clone();
                async move { deliver_event(&app, &group, &payload).await.map_err(|_| ()) }
            },
        )
        .await;
    })
}

/// Process one topic event (tests and bridge).
pub async fn deliver_event(
    app: &TrembitaApp,
    group: &str,
    payload: &[u8],
) -> Result<(), super::CapError> {
    let wire: CapWire = proto::decode(payload).map_err(super::CapError::codec)?;
    let Some(binding) = app.cap_runtime().binding(group, &wire.op) else {
        return Err(super::CapError::NotRegistered {
            group: group.to_string(),
            op: wire.op,
        });
    };
    if !binding
        .routes
        .iter()
        .any(|r| matches!(r, super::Route::Event))
    {
        return Err(super::CapError::UnsupportedRoute {
            route: super::Route::Event,
            op: wire.op.clone(),
        });
    }
    let bytes = encode(&wire).map_err(super::CapError::codec)?;
    let _reply = if let Some(key_fn) = &binding.key {
        if let Some(key) = key_fn(&wire.payload) {
            app.cluster()
                .messaging()
                .ask_keyed(group, &key, bytes)
                .await
                .map_err(|e| super::CapError::Deliver(e.to_string()))?
        } else {
            app.cluster()
                .messaging()
                .ask(group, bytes)
                .await
                .map_err(|e| super::CapError::Deliver(e.to_string()))?
        }
    } else {
        app.cluster()
            .messaging()
            .ask(group, bytes)
            .await
            .map_err(|e| super::CapError::Deliver(e.to_string()))?
    };
    Ok(())
}
