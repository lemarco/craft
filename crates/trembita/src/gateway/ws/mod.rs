//! WebSocket helpers: raw upgrade, sticky sessions, broadcast and per-user push.
//!
//! Re-exports [`tokio_tungstenite`] and [`futures_util`] so product apps need only `trembita`.
//!
//! Wiring guide: [WebSocket wiring](../../../../docs/scenarios/websocket-wiring.md).

mod broadcast;
mod mount;
mod notify;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use http::Request;
use hyper::body::Incoming;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use trembita_http::{AuthMode, RouteTable, accept_websocket, routing_to_http_response};

pub use broadcast::{WsBroadcastHub, WsSubscribeCmd};
pub use mount::{WsMount, apply_ws_mounts};
pub use notify::WsNotifyHub;

pub use futures_util;
pub use tokio_tungstenite;
pub use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::session::SessionHandle;
use super::state::TrembitaGatewayState;
use trembita_http::UpgradeStream;

type WsTask = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(crate) type UpgradeResponse =
    http::Response<http_body_util::combinators::BoxBody<bytes::Bytes, std::convert::Infallible>>;

pub(crate) type UpgradeFuture = Pin<Box<dyn Future<Output = UpgradeResponse> + Send>>;

type UpgradeHandler = Arc<dyn Fn(Request<Incoming>) -> UpgradeFuture + Send + Sync>;

/// Upgraded TCP stream plus gateway state (call [`TrembitaGatewayState::track_connection`]).
pub struct RawWs {
    /// Product gateway state (actors, identity, drain).
    pub state: TrembitaGatewayState,
    /// Hyper upgraded stream — wrap with [`WebSocketStream::from_raw_socket`].
    pub stream: UpgradeStream,
}

/// Sticky actor session opened before the WebSocket loop runs.
pub struct StickyWs {
    /// Gateway state.
    pub state: TrembitaGatewayState,
    /// Upgraded stream.
    pub stream: UpgradeStream,
    /// Session routing key (from gateway identity / session key mapping).
    pub session_key: String,
    /// Handle for cast/ask to the pinned worker.
    pub handle: SessionHandle,
}

pub(crate) fn register_websocket(
    table: RouteTable,
    path: &str,
    auth: AuthMode,
    handler: UpgradeHandler,
) -> RouteTable {
    let handler = move |req: Request<Incoming>| {
        let handler = Arc::clone(&handler);
        Box::pin(async move { handler(req).await })
            as Pin<Box<dyn Future<Output = UpgradeResponse> + Send>>
    };
    match auth {
        AuthMode::Open => table.websocket(path, handler),
        AuthMode::Session => table.websocket_session(path, handler),
        AuthMode::Identity => table.websocket_identity(path, handler),
    }
}

/// Mount a WebSocket at `path`; `on_connected` runs after a successful upgrade.
#[must_use]
pub fn mount_raw_websocket(
    table: RouteTable,
    path: &str,
    auth: AuthMode,
    state: TrembitaGatewayState,
    on_connected: impl Fn(RawWs) -> WsTask + Send + Sync + 'static,
) -> RouteTable {
    let on_connected = Arc::new(on_connected);
    let handler: UpgradeHandler = Arc::new(move |req| {
        let state = state.clone();
        let on_connected = Arc::clone(&on_connected);
        Box::pin(async move {
            accept_websocket(req, move |stream| {
                let state = state.clone();
                let on_connected = Arc::clone(&on_connected);
                async move {
                    let _guard = state.track_connection();
                    on_connected(RawWs {
                        state: state.clone(),
                        stream,
                    })
                    .await;
                }
            })
        }) as UpgradeFuture
    });
    register_websocket(table, path, auth, handler)
}

/// Open a sticky [`SessionHandle`] for `group`, then run `on_connected`.
#[must_use]
pub fn mount_sticky_websocket(
    table: RouteTable,
    path: &str,
    auth: AuthMode,
    state: TrembitaGatewayState,
    group: &str,
    ttl: Option<Duration>,
    on_connected: impl Fn(StickyWs) -> WsTask + Send + Sync + 'static,
) -> RouteTable {
    let group = group.to_string();
    let on_connected = Arc::new(on_connected);
    let handler: UpgradeHandler = Arc::new(move |req| {
        let state = state.clone();
        let group = group.clone();
        let on_connected = Arc::clone(&on_connected);
        Box::pin(async move {
            match state
                .open_actor_session_parts(&group, req.method(), req.uri(), req.headers(), ttl)
                .await
            {
                Ok(handle) => {
                    let session_key = handle.session_key().to_string();
                    accept_websocket(req, move |stream| {
                        let state = state.clone();
                        let on_connected = Arc::clone(&on_connected);
                        async move {
                            let _guard = state.track_connection();
                            on_connected(StickyWs {
                                state: state.clone(),
                                stream,
                                session_key,
                                handle,
                            })
                            .await;
                        }
                    })
                }
                Err(err) => routing_to_http_response(&err.into_http_response()),
            }
        }) as UpgradeFuture
    });
    register_websocket(table, path, auth, handler)
}

/// Text loop with sticky [`SessionHandle`] — `on_text` returns optional reply (sync).
pub async fn run_sticky_text_loop<S, F>(
    mut ws: WebSocketStream<S>,
    handle: &mut SessionHandle,
    mut on_text: F,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(&mut SessionHandle, String) -> Option<String>,
{
    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg
            && let Some(reply) = on_text(handle, text.to_string())
            && ws.send(WsMessage::Text(reply.into())).await.is_err()
        {
            break;
        }
    }
}

/// Sticky loop: text frame → `proto` encode → [`SessionHandle::cast`] → `ok: {text}` or error text.
pub async fn run_sticky_cast_loop<S>(
    mut ws: WebSocketStream<S>,
    handle: &mut SessionHandle,
    mut on_frame: impl FnMut(&str, bool) + Send,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg {
            let text = text.to_string();
            let payload = match trembita_proto::encode(&text) {
                Ok(p) => p,
                Err(e) => {
                    on_frame(&text, false);
                    let _ = ws
                        .send(WsMessage::Text(format!("encode error: {e}").into()))
                        .await;
                    continue;
                }
            };
            match handle.cast(payload).await {
                Ok(()) => {
                    on_frame(&text, true);
                    if ws
                        .send(WsMessage::Text(format!("ok: {text}").into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(e) => {
                    on_frame(&text, false);
                    if ws
                        .send(WsMessage::Text(format!("session error: {e}").into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }
}

/// Run a text-in / text-out loop: each inbound text frame is passed to `on_text`.
pub async fn run_text_loop<S, F, Fut>(mut ws: WebSocketStream<S>, mut on_text: F)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Option<String>> + Send,
{
    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg
            && let Some(reply) = on_text(text.to_string()).await
            && ws.send(WsMessage::Text(reply.into())).await.is_err()
        {
            break;
        }
    }
}

/// Wrap an upgraded stream as a server [`WebSocketStream`].
pub async fn server_stream(stream: UpgradeStream) -> WebSocketStream<UpgradeStream> {
    WebSocketStream::from_raw_socket(stream, Role::Server, None).await
}
