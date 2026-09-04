use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, Method, Uri};

use super::super::app::TrembitaApp;
use super::drain::{ConnectionGuard, ConnectionTracker};
use super::identity::{
    self, ExtractedIdentity, GatewayIdentity, GatewayRequest, IdentityError, SessionKey,
};
use super::session::{NoWorkerError, OpenActorSessionError, SessionHandle};

/// Shared Axum state for gateway handlers that need the running app.
#[derive(Clone)]
pub struct TrembitaGatewayState {
    /// Running product app handle.
    pub app: Arc<TrembitaApp>,
    identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
    connections: Option<Arc<ConnectionTracker>>,
}

impl fmt::Debug for TrembitaGatewayState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrembitaGatewayState")
            .field("identity", &self.identity.as_ref().map(|_| "<extractor>"))
            .field(
                "connections",
                &self.connections.as_ref().map_or(0, |c| c.active()),
            )
            .finish_non_exhaustive()
    }
}

impl TrembitaGatewayState {
    /// Gateway state with only the app handle (no identity extractor).
    #[must_use]
    pub fn new(app: Arc<TrembitaApp>) -> Self {
        Self {
            app,
            identity: None,
            connections: None,
        }
    }

    /// Track a long-lived connection until the guard drops (WebSocket, SSE, …).
    ///
    /// Short HTTP handlers are tracked automatically by gateway middleware when the
    /// router is built via [`super::build_gateway_router`] or [`super::spawn_gateway`].
    #[must_use]
    pub fn track_connection(&self) -> Option<ConnectionGuard<'_>> {
        self.connections.as_ref().map(|c| c.track())
    }

    /// Extract authenticated identity and session key.
    ///
    /// # Errors
    /// Returns [`IdentityError::NotConfigured`] when no extractor was set, or the extractor's error.
    pub async fn extract_session(
        &self,
        req: &GatewayRequest<'_>,
    ) -> Result<ExtractedIdentity, IdentityError> {
        match &self.identity {
            Some(extractor) => extractor.extract_dyn(req).await,
            None => Err(IdentityError::NotConfigured),
        }
    }

    /// [`extract_session`](Self::extract_session) from any HTTP request.
    ///
    /// # Errors
    /// Same as [`Self::extract_session`].
    pub async fn extract_session_from<B>(
        &self,
        req: &axum::http::Request<B>,
    ) -> Result<ExtractedIdentity, IdentityError> {
        let gw_req = GatewayRequest::from_http(req);
        self.extract_session(&gw_req).await
    }

    /// Auth + sticky [`SessionHandle`] in one call.
    ///
    /// # Errors
    /// Returns identity errors, or [`OpenActorSessionError::NoWorker`] when no worker is available.
    pub async fn open_actor_session(
        &self,
        group: &str,
        req: &GatewayRequest<'_>,
        ttl: Option<Duration>,
    ) -> Result<SessionHandle, OpenActorSessionError> {
        let extracted = self.extract_session(req).await?;
        SessionHandle::open_from_extracted(&self.app, group, &extracted, ttl)
            .ok_or_else(|| NoWorkerError(group.to_string()).into())
    }

    /// Like [`Self::open_actor_session`] from any HTTP request.
    ///
    /// # Errors
    /// Same as [`Self::open_actor_session`].
    pub async fn open_actor_session_from<B>(
        &self,
        group: &str,
        req: &axum::http::Request<B>,
        ttl: Option<Duration>,
    ) -> Result<SessionHandle, OpenActorSessionError> {
        let gw_req = GatewayRequest::from_http(req);
        self.open_actor_session(group, &gw_req, ttl).await
    }

    /// [`extract_session`](Self::extract_session) from axum **parts** (WebSocket upgrade handlers).
    ///
    /// Use with [`WebSocketUpgrade`](axum::extract::ws::WebSocketUpgrade) — do not also extract
    /// [`Request`](axum::http::Request); the upgrade consumes the body.
    ///
    /// # Errors
    /// Same as [`Self::extract_session`].
    pub async fn extract_session_parts(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Result<ExtractedIdentity, IdentityError> {
        let gw_req = GatewayRequest::from_parts(method, uri, headers);
        self.extract_session(&gw_req).await
    }

    /// Like [`Self::open_actor_session`] from axum **parts** (WebSocket upgrade handlers).
    ///
    /// # Errors
    /// Same as [`Self::open_actor_session`].
    pub async fn open_actor_session_parts(
        &self,
        group: &str,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        ttl: Option<Duration>,
    ) -> Result<SessionHandle, OpenActorSessionError> {
        let gw_req = GatewayRequest::from_parts(method, uri, headers);
        self.open_actor_session(group, &gw_req, ttl).await
    }

    /// Gateway state with app handle and identity extractor.
    #[must_use]
    pub fn with_identity<I>(app: Arc<TrembitaApp>, extractor: I) -> Self
    where
        I: GatewayIdentity,
        I::Identity: SessionKey,
    {
        Self {
            app,
            identity: Some(identity::erase_identity(extractor)),
            connections: None,
        }
    }

    /// Like [`Self::with_identity`] but map session key separately from identity.
    #[must_use]
    pub fn with_identity_mapped<I, F>(app: Arc<TrembitaApp>, extractor: I, session_key: F) -> Self
    where
        I: GatewayIdentity,
        I::Identity: 'static,
        F: Fn(&I::Identity) -> String + Send + Sync + 'static,
    {
        Self {
            app,
            identity: Some(identity::erase_identity_mapped(extractor, session_key)),
            connections: None,
        }
    }

    pub(crate) fn from_parts(
        app: Arc<TrembitaApp>,
        identity: Option<Arc<dyn identity::DynGatewayIdentity>>,
        connections: Option<Arc<ConnectionTracker>>,
    ) -> Self {
        Self {
            app,
            identity,
            connections,
        }
    }
}
