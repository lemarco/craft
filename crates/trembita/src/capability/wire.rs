//! On-the-wire frames for [`super::host::CapHost`].

use serde::{Deserialize, Serialize};

/// Frame delivered to a capability group host (inline cast/ask).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapWire {
    /// Registered op name within the group.
    pub op: String,
    /// Postcard-encoded request body for that op.
    pub payload: Vec<u8>,
}

/// Job payload for [`super::Route::Queued`] — consumer forwards to inline ask.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapQueued {
    /// Target capability group.
    pub group: String,
    /// Op name.
    pub op: String,
    /// Postcard-encoded request.
    pub body: Vec<u8>,
    /// When true, the queue bridge stores the inline reply for [`super::Route::QueuedWait`].
    #[serde(default)]
    pub wait: bool,
}
