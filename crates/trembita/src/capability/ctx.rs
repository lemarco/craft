//! Handler context.

use std::sync::Arc;

use crate::TrembitaApp;
use crate::capstore::CapStore;

/// Context passed to capability op handlers.
pub struct OpCtx<'a> {
    app: Option<&'a TrembitaApp>,
}

impl<'a> OpCtx<'a> {
    /// Build context for an in-process handler (no app handle).
    #[must_use]
    pub fn without_app() -> Self {
        Self { app: None }
    }

    /// Build context with a running app handle (queue bridge, HTTP adapters).
    #[must_use]
    pub fn with_app(app: &'a TrembitaApp) -> Self {
        Self { app: Some(app) }
    }

    /// Running app, when the invocation path provides one.
    #[must_use]
    pub fn app(&self) -> Option<&'a TrembitaApp> {
        self.app
    }

    /// Embedded workflow store when the app was booted with [`TrembitaApp`](crate::TrembitaApp) + `data_dir`.
    #[must_use]
    pub fn cap_store(&self) -> Option<Arc<dyn CapStore>> {
        self.app.map(TrembitaApp::cap_store).flatten()
    }
}
