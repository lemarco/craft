//! Sticky [`SessionHandle`] — open / cast / ask with automatic session reopen.

use std::sync::Arc;
use std::time::Duration;

use crafty_actor::{ActorSession, CastError, ClusterAskError};

use super::identity::{ExtractedIdentity, IdentityError};
use crate::app::CraftyApp;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

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

impl IntoResponse for OpenActorSessionError {
    fn into_response(self) -> Response {
        match self {
            Self::Identity(err) => err.into_response(),
            Self::NoWorker(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
        }
    }
}

/// Gateway-side sticky session to one worker instance (reopen on `NoTarget`).
pub struct SessionHandle {
    app: Arc<CraftyApp>,
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
    /// Open a sticky session for `session_key` in worker group `group`.
    #[must_use]
    pub fn open(
        app: &Arc<CraftyApp>,
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
        app: &Arc<CraftyApp>,
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

    /// Ask with one automatic reopen when the target is gone or expired.
    ///
    /// # Errors
    /// Returns [`ClusterAskError`] when no worker is available or delivery fails.
    pub async fn ask(&mut self, payload: Vec<u8>) -> Result<Vec<u8>, ClusterAskError> {
        self.ask_with_retries(payload, 1).await
    }

    async fn cast_with_retries(
        &mut self,
        payload: Vec<u8>,
        retries: u8,
    ) -> Result<(), CastError> {
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
