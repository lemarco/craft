//! Standard bootstrap payloads: cron (or HTTP) → queue consumer → workflow or follow-up enqueue.
//!
//! Product apps may use their own payload enums; these wire shapes are optional conveniences.

use serde::{Deserialize, Serialize};

/// Errors encoding or decoding a [`WorkTrigger`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkTriggerError {
    /// JSON could not be parsed as a trigger.
    #[error("invalid work trigger json: {0}")]
    Json(String),
    /// Unsupported wire version.
    #[error("unsupported work trigger version {0}")]
    Version(u32),
}

/// Built-in bootstrap actions for orchestration stream consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkTrigger {
    /// Run a registered workflow by saga id (`TrembitaApp::run_workflow_id` on the product facade).
    Workflow {
        /// Saga id registered with `.workflows([…])`.
        saga_id: String,
    },
    /// Enqueue a follow-up job on another stream (pipeline bootstrap).
    Enqueue {
        /// Target stream name.
        stream: String,
        /// Job body (UTF-8 or opaque bytes in JSON array form).
        payload: Vec<u8>,
    },
}

#[derive(Serialize, Deserialize)]
struct WireEnvelope {
    v: u32,
    #[serde(flatten)]
    trigger: WorkTrigger,
}

impl WorkTrigger {
    /// Current wire version.
    pub const WIRE_VERSION: u32 = 1;

    /// Serialize to JSON bytes for `RecurringJob` / `enqueue` payloads.
    #[must_use]
    pub fn to_payload(&self) -> Vec<u8> {
        let wire = WireEnvelope {
            v: Self::WIRE_VERSION,
            trigger: self.clone(),
        };
        serde_json::to_vec(&wire).unwrap_or_default()
    }

    /// Parse JSON payload; returns `None` when bytes are not a [`WorkTrigger`].
    pub fn decode(payload: &[u8]) -> Result<Option<Self>, WorkTriggerError> {
        let wire: WireEnvelope = match serde_json::from_slice(payload) {
            Ok(w) => w,
            Err(_) => return Ok(None),
        };
        if wire.v != Self::WIRE_VERSION {
            return Err(WorkTriggerError::Version(wire.v));
        }
        Ok(Some(wire.trigger))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_roundtrip() {
        let trigger = WorkTrigger::Workflow {
            saga_id: "weekly-report".into(),
        };
        let bytes = trigger.to_payload();
        let decoded = WorkTrigger::decode(&bytes).expect("decode").expect("some");
        assert_eq!(decoded, trigger);
    }

    #[test]
    fn enqueue_roundtrip() {
        let trigger = WorkTrigger::Enqueue {
            stream: "seo-parse".into(),
            payload: br#"{"action":"start_run"}"#.to_vec(),
        };
        let bytes = trigger.to_payload();
        let decoded = WorkTrigger::decode(&bytes).expect("decode").expect("some");
        assert_eq!(decoded, trigger);
    }

    #[test]
    fn non_trigger_returns_none() {
        assert!(
            WorkTrigger::decode(b"plain email body")
                .expect("decode")
                .is_none()
        );
    }
}
