//! Handler context.

use crate::TrembitaApp;

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
}
