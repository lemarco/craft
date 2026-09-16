//! Capability invocation errors.

use std::fmt;

/// Error from capability registration, routing, or handler execution.
#[derive(Debug)]
pub enum CapError {
    /// Op or group is not registered.
    NotRegistered {
        /// Capability group name.
        group: String,
        /// Operation name within the group.
        op: String,
    },
    /// Requested [`super::Route`] is not enabled for this op.
    UnsupportedRoute {
        /// Route the caller requested.
        route: super::Route,
        /// Operation name.
        op: String,
    },
    /// Postcard encode/decode failure.
    Codec(String),
    /// Cluster mailbox delivery failed.
    Deliver(String),
    /// Handler returned an error message.
    Handler(String),
    /// Domain validation / business rule (maps to client error over HTTP).
    Domain {
        /// Human-readable message.
        message: String,
    },
    /// Queued route requires a queue stream on the group.
    MissingQueueStream {
        /// Group name.
        group: String,
    },
    /// [`super::Route::QueuedWait`] timed out waiting for the consumer reply.
    WaitTimeout {
        /// Job id.
        job_id: trembita_jobs::JobId,
    },
    /// Missing call-site option (session key, schedule time, …).
    MissingOption {
        /// What was required.
        detail: String,
    },
}

impl fmt::Display for CapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRegistered { group, op } => {
                write!(f, "capability op {group}.{op} is not registered")
            }
            Self::UnsupportedRoute { route, op } => {
                write!(f, "route {route:?} is not enabled for op {op}")
            }
            Self::Codec(e) => write!(f, "capability codec: {e}"),
            Self::Deliver(e) => write!(f, "capability deliver: {e}"),
            Self::Handler(e) => write!(f, "capability handler: {e}"),
            Self::Domain { message } => write!(f, "capability domain: {message}"),
            Self::MissingQueueStream { group } => {
                write!(
                    f,
                    "capability group {group} has no queue_stream for Route::Queued"
                )
            }
            Self::WaitTimeout { job_id } => {
                write!(f, "capability queued wait timed out for job {}", job_id.0)
            }
            Self::MissingOption { detail } => write!(f, "capability call: {detail}"),
        }
    }
}

impl std::error::Error for CapError {}

impl CapError {
    pub(crate) fn codec(e: impl fmt::Display) -> Self {
        Self::Codec(e.to_string())
    }

    /// Map a domain error into [`CapError::Domain`] (preferred over [`Self::Handler`] for validation).
    #[must_use]
    pub fn domain(message: impl Into<String>) -> Self {
        Self::Domain {
            message: message.into(),
        }
    }

    /// Wrap any displayable error as a handler failure.
    #[must_use]
    pub fn handler(err: impl fmt::Display) -> Self {
        Self::Handler(err.to_string())
    }
}
