//! Call-site metadata propagated into handlers ([`OpCtx::ingress`](super::ctx::OpCtx::ingress)).

use serde::{Deserialize, Serialize};

/// Gateway or in-process caller context for audit and tracing.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapIngress {
    /// End-user or service principal (Bearer subject, session user, …).
    #[serde(default)]
    pub principal: Option<String>,
    /// Correlation / request id (`X-Request-Id`, trace id, …).
    #[serde(default)]
    pub correlation_id: Option<String>,
}

impl CapIngress {
    /// Empty ingress — default for queue bridge and tests.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }
}
