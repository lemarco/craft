//! Topic broadcast hub for open WebSocket clients.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use trembita_http::{AuthMode, RouteTable};

use super::{RawWs, WsMessage, mount_raw_websocket};
use crate::gateway::TrembitaGatewayState;
use trembita_http::UpgradeStream;

/// Client → server subscribe/unsubscribe (JSON text frame).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WsSubscribeCmd {
    /// Subscribe to `topic`.
    Subscribe {
        /// Topic id (e.g. `BTC`).
        topic: String,
    },
    /// Stop receiving `topic`.
    Unsubscribe {
        /// Topic id.
        topic: String,
    },
}

/// Fan-out hub: clients send [`WsSubscribeCmd`] JSON text frames.
#[derive(Clone, Default)]
pub struct WsBroadcastHub {
    inner: Arc<BroadcastInner>,
}

#[derive(Default)]
struct BroadcastInner {
    topics: Mutex<HashMap<String, tokio::sync::broadcast::Sender<String>>>,
}

impl WsBroadcastHub {
    /// Empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish `message` to every connection subscribed to `topic`.
    pub fn publish(&self, topic: &str, message: impl Into<String>) {
        let msg = message.into();
        let tx = self
            .inner
            .topics
            .lock()
            .expect("broadcast lock")
            .get(topic)
            .cloned();
        if let Some(tx) = tx {
            let _ = tx.send(msg);
        }
    }

    /// Open WebSocket at `path`.
    #[must_use]
    pub fn mount(
        &self,
        table: RouteTable,
        path: &str,
        auth: AuthMode,
        state: TrembitaGatewayState,
    ) -> RouteTable {
        let hub = self.clone();
        mount_raw_websocket(table, path, auth, state, move |raw| {
            let hub = hub.clone();
            Box::pin(async move { run_broadcast_client(hub, raw).await })
        })
    }
}

async fn run_broadcast_client(hub: WsBroadcastHub, raw: RawWs) {
    let mut ws = WebSocketStream::from_raw_socket(raw.stream, Role::Server, None).await;
    let mut subs: HashMap<String, tokio::sync::broadcast::Receiver<String>> = HashMap::new();
    loop {
        if subs.is_empty() {
            match ws.next().await {
                Some(Ok(WsMessage::Text(text))) => apply_broadcast_cmd(&hub, &mut subs, &text),
                Some(Ok(WsMessage::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(_)) => break,
            }
            continue;
        }
        tokio::select! {
            frame = ws.next() => {
                match frame {
                    Some(Ok(WsMessage::Text(text))) => {
                        apply_broadcast_cmd(&hub, &mut subs, &text);
                    }
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            text = recv_subscribed(&mut subs) => {
                if ws.send(WsMessage::Text(text.into())).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn recv_subscribed(
    subs: &mut HashMap<String, tokio::sync::broadcast::Receiver<String>>,
) -> String {
    loop {
        for rx in subs.values_mut() {
            if let Ok(msg) = rx.try_recv() {
                return msg;
            }
        }
        if let Some((_topic, rx)) = subs.iter_mut().next() {
            if let Ok(msg) = rx.recv().await {
                return msg;
            }
        }
        tokio::task::yield_now().await;
    }
}

fn apply_broadcast_cmd(
    hub: &WsBroadcastHub,
    subs: &mut HashMap<String, tokio::sync::broadcast::Receiver<String>>,
    text: &str,
) {
    let Ok(cmd) = serde_json::from_str::<WsSubscribeCmd>(text) else {
        return;
    };
    match cmd {
        WsSubscribeCmd::Subscribe { topic } => {
            let tx = hub
                .inner
                .topics
                .lock()
                .expect("broadcast lock")
                .entry(topic.clone())
                .or_insert_with(|| {
                    let (tx, _) = tokio::sync::broadcast::channel(256);
                    tx
                })
                .clone();
            subs.insert(topic, tx.subscribe());
        }
        WsSubscribeCmd::Unsubscribe { topic } => {
            subs.remove(&topic);
        }
    }
}
