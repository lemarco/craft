//! Async handler trait and function adapters.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::ctx::{RequestCtx, Response};
use super::error::HttpError;

/// Async HTTP handler invoked after routing and auth gates.
pub trait Handler: Send + Sync {
    /// Handle one request.
    fn handle(
        &self,
        ctx: RequestCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Response, HttpError>> + Send>>;
}

impl<F, Fut> Handler for F
where
    F: Fn(RequestCtx) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Response, HttpError>> + Send + 'static,
{
    fn handle(
        &self,
        ctx: RequestCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Response, HttpError>> + Send>> {
        Box::pin((self)(ctx))
    }
}

/// Type-erased handler for storage in [`super::RouteTable`](super::RouteTable).
#[derive(Clone)]
pub struct ArcHandler(Arc<dyn Handler>);

impl ArcHandler {
    /// Wrap any handler for route table storage.
    #[must_use]
    pub fn new<H: Handler + 'static>(handler: H) -> Self {
        Self(Arc::new(handler))
    }

    /// Dispatch to the inner handler.
    pub fn handle(
        &self,
        ctx: RequestCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Response, HttpError>> + Send>> {
        self.0.handle(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use http::{Method, StatusCode};

    use super::*;
    use crate::routing::path::PathParams;

    #[tokio::test]
    async fn fn_handler_receives_context() {
        let handler = |ctx: RequestCtx| async move {
            Ok(Response::text(
                StatusCode::OK,
                format!("path={}", ctx.path()),
            ))
        };
        let wrapped = ArcHandler::new(handler);
        let ctx = RequestCtx::new(
            Method::GET,
            "/hello",
            PathParams::new(),
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        let resp = wrapped.handle(ctx).await.expect("ok");
        assert_eq!(resp.status_code(), StatusCode::OK);
    }
}
