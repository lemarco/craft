//! Per-user notification hub (identity-gated WebSocket).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use trembita_http::{RouteTable, accept_websocket, routing_to_http_response};

use super::WsMessage;
use crate::gateway::TrembitaGatewayState;
use trembita_http::UpgradeStream;

type UpgradeHandler = std::sync::Arc<
    dyn Fn(http::Request<hyper::body::Incoming>) -> super::UpgradeFuture + Send + Sync,
>;

/// Per-user push: identity must succeed before upgrade; [`Self::publish_to`] targets one session key.
#[derive(Clone, Default)]
pub struct WsNotifyHub {
    inner: Arc<NotifyInner>,
}

#[derive(Default)]
struct NotifyInner {
    users: Mutex<HashMap<String, Vec<tokio::sync::mpsc::UnboundedSender<String>>>>,
}

impl WsNotifyHub {
    /// Empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Push `message` to every open WebSocket for `session_key`.
    pub fn publish_to(&self, session_key: &str, message: impl Into<String>) {
        let msg = message.into();
        let senders = self
            .inner
            .users
            .lock()
            .expect("notify lock")
            .get(session_key)
            .cloned();
        if let Some(senders) = senders {
            for tx in senders {
                let _ = tx.send(msg.clone());
            }
        }
    }

    /// WebSocket protected by gateway identity.
    #[must_use]
    pub fn mount(&self, table: RouteTable, path: &str, state: TrembitaGatewayState) -> RouteTable {
        let hub = self.clone();
        let handler: UpgradeHandler = Arc::new(move |req| {
            let state = state.clone();
            let hub = hub.clone();
            Box::pin(async move {
                let extracted = match state
                    .extract_session_parts(req.method(), req.uri(), req.headers())
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        return routing_to_http_response(&err.into_http_response());
                    }
                };
                let session_key = extracted.session_key().to_string();
                let track = state.clone();
                accept_websocket(req, move |stream| {
                    let hub = hub.clone();
                    let session_key = session_key.clone();
                    async move {
                        let _guard = track.track_connection();
                        run_notify_client(hub, session_key, stream).await;
                    }
                })
            }) as super::UpgradeFuture
        });
        super::register_websocket(table, path, trembita_http::AuthMode::Identity, handler)
    }
}

async fn run_notify_client(hub: WsNotifyHub, session_key: String, stream: UpgradeStream) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    hub.inner
        .users
        .lock()
        .expect("notify lock")
        .entry(session_key.clone())
        .or_default()
        .push(tx);

    let mut ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
    let _ = ws
        .send(WsMessage::Text(
            format!("notify channel open for {session_key}").into(),
        ))
        .await;

    loop {
        tokio::select! {
            frame = ws.next() => {
                match frame {
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Ok(WsMessage::Text(_))) => {}
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(text) => {
                        if ws.send(WsMessage::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    let mut users = hub.inner.users.lock().expect("notify lock");
    if let Some(list) = users.get_mut(&session_key) {
        list.retain(|s| !s.is_closed());
        if list.is_empty() {
            users.remove(&session_key);
        }
    }
}
