//! Sticky [`SessionHandle`] — open / cast / ask with automatic session reopen.

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use trembita_http::Response;
use trembita_runtime::{
    ActorSession, CastError, ClusterAskError, DeliverError, MessageDecodeError,
};

use super::identity::{ExtractedIdentity, IdentityError};
use crate::app::TrembitaApp;
use crate::capability::{CapRequest, cap_wire_bytes};

/// No worker available for the session key in the requested group.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no worker for group {0}")]
pub struct NoWorkerError(pub String);

/// Opening an actor session failed (auth or no worker).
#[derive(Debug, thiserror::Error)]
pub enum OpenActorSessionError {
    /// Identity extraction failed.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// No worker registered for the group / session key.
    #[error(transparent)]
    NoWorker(#[from] NoWorkerError),
}

impl OpenActorSessionError {
    /// Map to a gateway [`Response`].
    #[must_use]
    pub fn into_http_response(self) -> Response {
        match self {
            Self::Identity(err) => err.into_http_response(),
            Self::NoWorker(err) => Response::text(StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
        }
    }
}

/// Gateway-side sticky session to one worker instance (reopen on `NoTarget`).
pub struct SessionHandle {
    app: Arc<TrembitaApp>,
    group: String,
    session_key: String,
    ttl: Option<Duration>,
    session: Option<ActorSession>,
}

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("group", &self.group)
            .field("session_key", &self.session_key)
            .field("ttl", &self.ttl)
            .field("has_session", &self.session.is_some())
            .finish_non_exhaustive()
    }
}

impl SessionHandle {
    /// Open a sticky session for `session_key` in [`CapRequest::GROUP`].
    #[must_use]
    pub fn open_for<Req: CapRequest>(
        app: &Arc<TrembitaApp>,
        session_key: impl Into<String>,
        ttl: Option<Duration>,
    ) -> Option<Self> {
        Self::open(app, Req::GROUP, session_key, ttl)
    }

    /// Open a sticky session for `session_key` in worker group `group`.
    #[must_use]
    pub fn open(
        app: &Arc<TrembitaApp>,
        group: impl Into<String>,
        session_key: impl Into<String>,
        ttl: Option<Duration>,
    ) -> Option<Self> {
        let group = group.into();
        let session_key = session_key.into();
        let session = app.session_str(&group, &session_key, ttl)?;
        Some(Self {
            app: Arc::clone(app),
            group,
            session_key,
            ttl,
            session: Some(session),
        })
    }

    /// Open from a prior [`ExtractedIdentity`].
    #[must_use]
    pub fn open_from_extracted(
        app: &Arc<TrembitaApp>,
        group: impl Into<String>,
        extracted: &ExtractedIdentity,
        ttl: Option<Duration>,
    ) -> Option<Self> {
        Self::open(app, group, extracted.session_key(), ttl)
    }

    /// Current actor session handle, if open.
    #[must_use]
    pub fn session(&self) -> Option<&ActorSession> {
        self.session.as_ref()
    }

    /// Worker group name.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Session key used for consistent-hash worker pick.
    #[must_use]
    pub fn session_key(&self) -> &str {
        &self.session_key
    }

    /// Re-open session after expiry or worker loss.
    pub fn reopen(&mut self) -> bool {
        self.session = self
            .app
            .session_str(&self.group, &self.session_key, self.ttl);
        self.session.is_some()
    }

    /// Cast with one automatic reopen when the target is gone or expired.
    ///
    /// # Errors
    /// Returns [`CastError`] when no worker is available after reopen.
    pub async fn cast(&mut self, payload: Vec<u8>) -> Result<(), CastError> {
        self.cast_with_retries(payload, 1).await
    }

    /// Fire-and-forget capability op on this sticky session ([`Route::InlineFire`](crate::capability::Route) path).
    ///
    /// # Errors
    /// Returns [`CastError`] when the request group does not match, encoding fails, or delivery fails.
    pub async fn fire_cap<Req: CapRequest>(&mut self, req: Req) -> Result<(), CastError> {
        let payload = cap_wire_for_session::<Req>(&self.group, &req)?;
        self.cast(payload).await
    }

    /// Capability op on this sticky session with reply ([`Route::Session`](crate::capability::Route) ask path).
    ///
    /// # Errors
    /// Returns [`ClusterAskError`] when the request group does not match, encoding fails, or delivery fails.
    pub async fn ask_cap<Req: CapRequest>(
        &mut self,
        req: Req,
    ) -> Result<Req::Reply, ClusterAskError> {
        let payload = cap_wire_for_session(&self.group, &req).map_err(cast_err_to_ask)?;
        let bytes = self.ask(payload).await?;
        trembita_proto::decode(&bytes).map_err(|_| ClusterAskError::NoReply)
    }

    /// Ask with one automatic reopen when the target is gone or expired.
    ///
    /// # Errors
    /// Returns [`ClusterAskError`] when no worker is available or delivery fails.
    pub async fn ask(&mut self, payload: Vec<u8>) -> Result<Vec<u8>, ClusterAskError> {
        self.ask_with_retries(payload, 1).await
    }

    async fn cast_with_retries(&mut self, payload: Vec<u8>, retries: u8) -> Result<(), CastError> {
        for attempt in 0..=retries {
            let Some(session) = self.session.as_ref() else {
                if !self.reopen() {
                    return Err(CastError::NoTarget(self.group.clone()));
                }
                continue;
            };
            match self.app.cast_session(session, payload.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < retries && session_recoverable(&e) => {
                    self.reopen();
                    if self.session.is_none() {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(CastError::NoTarget(self.group.clone()))
    }

    async fn ask_with_retries(
        &mut self,
        payload: Vec<u8>,
        retries: u8,
    ) -> Result<Vec<u8>, ClusterAskError> {
        for attempt in 0..=retries {
            let Some(session) = self.session.as_ref() else {
                if !self.reopen() {
                    return Err(ClusterAskError::NoTarget(self.group.clone()));
                }
                continue;
            };
            match self.app.ask_session(session, payload.clone()).await {
                Ok(reply) => return Ok(reply),
                Err(e) if attempt < retries && ask_session_recoverable(&e) => {
                    self.reopen();
                    if self.session.is_none() {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(ClusterAskError::NoTarget(self.group.clone()))
    }
}

fn session_recoverable(err: &CastError) -> bool {
    matches!(err, CastError::NoTarget(_))
        || err.to_string().contains("NoTarget")
        || err.to_string().contains("expired")
}

fn ask_session_recoverable(err: &ClusterAskError) -> bool {
    matches!(err, ClusterAskError::NoTarget(_))
        || err.to_string().contains("NoTarget")
        || err.to_string().contains("expired")
}

fn cap_wire_for_session<Req: CapRequest>(
    session_group: &str,
    req: &Req,
) -> Result<Vec<u8>, CastError> {
    if session_group != Req::GROUP {
        return Err(CastError::Deliver(DeliverError::NotFound(format!(
            "session group `{session_group}` != CapRequest::GROUP `{}`",
            Req::GROUP
        ))));
    }
    cap_wire_bytes(req).map_err(cap_encode_error)
}

fn cast_err_to_ask(err: CastError) -> ClusterAskError {
    match err {
        CastError::NoTarget(g) => ClusterAskError::NoTarget(g),
        CastError::Deliver(d) => ClusterAskError::Deliver(d),
        CastError::Remote(r) => ClusterAskError::Remote(r),
    }
}

fn cap_encode_error(err: crate::capability::CapError) -> CastError {
    CastError::Deliver(DeliverError::Decode(MessageDecodeError::Decode(
        err.to_string(),
    )))
}
