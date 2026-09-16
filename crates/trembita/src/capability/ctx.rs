//! Handler context.

use std::sync::Arc;

use super::ingress::CapIngress;
use crate::TrembitaApp;
use crate::capstore::CapStore;

/// Context passed to capability op handlers.
pub struct OpCtx<'a> {
    app: Option<&'a TrembitaApp>,
    ingress: Option<&'a CapIngress>,
}

impl<'a> OpCtx<'a> {
    /// Build context for an in-process handler (no app handle).
    #[must_use]
    pub fn without_app() -> Self {
        Self {
            app: None,
            ingress: None,
        }
    }

    /// Build context with a running app handle (queue bridge, HTTP adapters).
    #[must_use]
    pub fn with_app(app: &'a TrembitaApp) -> Self {
        Self {
            app: Some(app),
            ingress: None,
        }
    }

    pub(crate) fn for_invocation(
        app: Option<&'a TrembitaApp>,
        ingress: Option<&'a CapIngress>,
    ) -> Self {
        Self { app, ingress }
    }

    /// Running app, when the invocation path provides one.
    #[must_use]
    pub fn app(&self) -> Option<&'a TrembitaApp> {
        self.app
    }

    /// Caller metadata when the delivery path attached [`CapIngress`].
    #[must_use]
    pub fn ingress(&self) -> Option<&CapIngress> {
        self.ingress
    }

    /// Builder-injected domain ports ([`TrembitaAppBuilder::cap_deps`]).
    #[must_use]
    pub fn deps<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.app.and_then(|a| a.cap_deps().get())
    }

    /// Embedded workflow store when the app was booted with [`TrembitaApp`](crate::TrembitaApp) + `data_dir`.
    #[must_use]
    pub fn cap_store(&self) -> Option<Arc<dyn CapStore>> {
        self.store()
    }

    /// Alias for [`Self::cap_store`] — same [`CapStore`](crate::capstore::CapStore) port.
    #[must_use]
    pub fn store(&self) -> Option<Arc<dyn CapStore>> {
        self.app.and_then(TrembitaApp::cap_store)
    }

    /// [`Self::store`] or a clear error when `data_dir` / durable store was not configured.
    ///
    /// # Errors
    /// [`CapError::MissingOption`](super::CapError::MissingOption) when no store is wired.
    pub fn require_store(&self) -> Result<Arc<dyn CapStore>, super::CapError> {
        self.store().ok_or_else(|| super::CapError::MissingOption {
            detail: "cap store requires TrembitaApp with data_dir (embedded redb)".into(),
        })
    }
}
