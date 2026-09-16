//! App-injected domain ports for capability handlers ([`OpCtx::deps`](super::ctx::OpCtx::deps)).

use std::any::Any;
use std::sync::Arc;

/// Type-erased handle to app-specific services (DB pools, HTTP clients, …).
#[derive(Clone, Default)]
pub struct CapDeps {
    inner: Option<Arc<dyn Any + Send + Sync>>,
}

impl CapDeps {
    /// Wrap a `'static` deps struct for handler lookup.
    #[must_use]
    pub fn new<T: Send + Sync + 'static>(deps: T) -> Self {
        Self {
            inner: Some(Arc::new(deps)),
        }
    }

    /// Borrow deps when the type matches what was registered on the builder.
    #[must_use]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.inner.as_ref()?.downcast_ref()
    }
}
