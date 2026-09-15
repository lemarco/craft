//! Declarative WebSocket mounts (broadcast / notify hubs).

use trembita_http::{AuthMode, RouteTable};

use super::broadcast::WsBroadcastHub;
use super::mount_raw_websocket;
use super::notify::WsNotifyHub;
use crate::gateway::TrembitaGatewayState;

/// Built-in WebSocket wiring (no custom handler — use [`super::mount_sticky_websocket`] for that).
#[derive(Clone)]
pub enum WsMount {
    /// Open or identity-protected echo on text frames.
    RawEcho {
        /// WebSocket path.
        path: String,
        /// Auth gate before upgrade.
        auth: AuthMode,
    },
    /// Topic fan-out ([`WsBroadcastHub`]).
    Broadcast {
        /// WebSocket path.
        path: String,
        /// Auth gate before upgrade.
        auth: AuthMode,
        /// Shared hub (clone is cheap).
        hub: WsBroadcastHub,
    },
    /// Per-user push ([`WsNotifyHub`], identity auth).
    Notify {
        /// WebSocket path.
        path: String,
        /// Shared hub.
        hub: WsNotifyHub,
    },
}

impl WsMount {
    /// Open echo at `path`.
    #[must_use]
    pub fn raw_echo(path: impl Into<String>) -> Self {
        Self::RawEcho {
            path: path.into(),
            auth: AuthMode::Open,
        }
    }

    /// Public ticker-style broadcast.
    #[must_use]
    pub fn broadcast(path: impl Into<String>, hub: WsBroadcastHub) -> Self {
        Self::Broadcast {
            path: path.into(),
            auth: AuthMode::Open,
            hub,
        }
    }

    /// Identity-gated notify channel.
    #[must_use]
    pub fn notify(path: impl Into<String>, hub: WsNotifyHub) -> Self {
        Self::Notify {
            path: path.into(),
            hub,
        }
    }
}

/// Apply declarative mounts; `state` is cloned per path as needed.
#[must_use]
pub fn apply_ws_mounts(
    mut table: RouteTable,
    state: TrembitaGatewayState,
    mounts: impl IntoIterator<Item = WsMount>,
) -> RouteTable {
    for mount in mounts {
        table = match mount {
            WsMount::RawEcho { path, auth } => {
                mount_raw_websocket(table, &path, auth, state.clone(), {
                    move |raw| {
                        Box::pin(async move {
                            let ws = super::server_stream(raw.stream).await;
                            super::run_text_loop(ws, |text| async move {
                                Some(format!("echo: {text}"))
                            })
                            .await;
                        })
                    }
                })
            }
            WsMount::Broadcast { path, auth, hub } => hub.mount(table, &path, auth, state.clone()),
            WsMount::Notify { path, hub } => hub.mount(table, &path, state.clone()),
        };
    }
    table
}
