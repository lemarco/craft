//! Single operation registration.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::call::CapRequest;
use super::ctx::OpCtx;
use super::error::CapError;
use super::host::{CapRegistry, OpRunner};
use super::route::Route;

pub(crate) type KeyFn = std::sync::Arc<dyn Fn(&[u8]) -> Option<String> + Send + Sync>;

type CapOpRegister<S> = Box<dyn Fn(&mut CapRegistry<S>) + Send + Sync>;

pub(crate) struct CapOpSpec<S: Send + Default + 'static> {
    pub(crate) name: &'static str,
    pub(crate) routes: Vec<Route>,
    pub(crate) key: Option<KeyFn>,
    register: CapOpRegister<S>,
}

impl<S: Send + Default + 'static> CapOpSpec<S> {
    pub(crate) fn install(&self, registry: &mut CapRegistry<S>) {
        (self.register)(registry);
    }
}

/// One registered operation within a capability group.
pub struct CapOp<S: Send + Default + 'static> {
    name: &'static str,
    routes: Vec<Route>,
    key: Option<KeyFn>,
    _state: PhantomData<S>,
    register: CapOpRegister<S>,
}

impl<S: Send + Default + 'static> CapOp<S> {
    /// Register a **sync** handler under `name`.
    #[must_use]
    pub fn new<Req, Reply>(name: &'static str, handler: CapHandlerFn<S, Req, Reply>) -> Self
    where
        Req: DeserializeOwned + Send + 'static,
        Reply: Serialize + Send + 'static,
    {
        let register: CapOpRegister<S> = Box::new(move |registry: &mut CapRegistry<S>| {
            registry.register(name, op_runner_sync(handler));
        });
        Self {
            name,
            routes: Vec::new(),
            key: None,
            _state: PhantomData,
            register,
        }
    }

    /// Register a sync handler; op name is [`CapRequest::OP`] on `Req`.
    #[must_use]
    pub fn for_request<Req, Reply>(handler: CapHandlerFn<S, Req, Reply>) -> Self
    where
        Req: CapRequest<Reply = Reply> + DeserializeOwned + Send + 'static,
        Reply: Serialize + Send + 'static,
    {
        Self::new(Req::OP, handler)
    }

    /// Register an **async** handler under `name`.
    ///
    /// Use a closure returning `Box::pin(async { ... })` (non-`move` async block when borrowing
    /// `state`).
    #[must_use]
    pub fn new_async<Req, Reply, H>(name: &'static str, handler: H) -> Self
    where
        Req: DeserializeOwned + Send + 'static,
        Reply: Serialize + Send + 'static,
        H: for<'a> Fn(
                Req,
                OpCtx<'a>,
                &'a mut S,
            ) -> std::pin::Pin<
                Box<dyn Future<Output = Result<Reply, CapError>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        let handler = Arc::new(handler);
        let register: CapOpRegister<S> = Box::new(move |registry: &mut CapRegistry<S>| {
            registry.register(name, op_runner_async(Arc::clone(&handler)));
        });
        Self {
            name,
            routes: Vec::new(),
            key: None,
            _state: PhantomData,
            register,
        }
    }

    /// Register an async handler; op name is [`CapRequest::OP`] on `Req`.
    #[must_use]
    pub fn for_request_async<Req, Reply, H>(handler: H) -> Self
    where
        Req: CapRequest<Reply = Reply> + DeserializeOwned + Send + 'static,
        Reply: Serialize + Send + 'static,
        H: for<'a> Fn(
                Req,
                OpCtx<'a>,
                &'a mut S,
            ) -> std::pin::Pin<
                Box<dyn Future<Output = Result<Reply, CapError>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        Self::new_async(Req::OP, handler)
    }

    /// Optional route whitelist (empty = all [`Route`] values allowed at call site).
    #[must_use]
    pub fn routes(mut self, routes: impl IntoIterator<Item = Route>) -> Self {
        self.routes = routes.into_iter().collect();
        self
    }

    /// Stable routing key from the decoded request (inline keyed ask/cast).
    #[must_use]
    pub fn key<Req: DeserializeOwned>(
        mut self,
        f: impl Fn(&Req) -> String + Send + Sync + 'static,
    ) -> Self {
        self.key = Some(std::sync::Arc::new(move |bytes: &[u8]| {
            trembita_proto::decode::<Req>(bytes).ok().map(|req| f(&req))
        }));
        self
    }

    /// Inline routing key from [`CapRequest::cap_key`](super::call::CapRequest::cap_key) on decode.
    #[must_use]
    pub fn key_cap<Req: CapRequest + DeserializeOwned>(mut self) -> Self {
        self.key = Some(std::sync::Arc::new(move |bytes: &[u8]| {
            trembita_proto::decode::<Req>(bytes)
                .ok()
                .and_then(|req| req.cap_key())
        }));
        self
    }

    pub(crate) fn into_spec(self) -> CapOpSpec<S> {
        CapOpSpec {
            name: self.name,
            routes: self.routes,
            key: self.key,
            register: self.register,
        }
    }
}

/// Sync handler signature.
pub type CapHandlerFn<S, Req, Reply> = fn(Req, OpCtx<'_>, &mut S) -> Result<Reply, CapError>;

fn op_runner_sync<S, Req, Reply>(handler: CapHandlerFn<S, Req, Reply>) -> OpRunner<S>
where
    S: Send + Default + 'static,
    Req: DeserializeOwned + Send + 'static,
    Reply: Serialize + Send + 'static,
{
    Arc::new(move |payload, state, ctx| {
        Box::pin(async move {
            let req: Req = trembita_proto::decode(&payload).map_err(CapError::codec)?;
            let reply = handler(req, ctx, state)?;
            trembita_proto::encode(&reply).map_err(CapError::codec)
        })
    })
}

fn op_runner_async<S, Req, Reply, H>(handler: Arc<H>) -> OpRunner<S>
where
    S: Send + Default + 'static,
    Req: DeserializeOwned + Send + 'static,
    Reply: Serialize + Send + 'static,
    H: for<'a> Fn(
            Req,
            OpCtx<'a>,
            &'a mut S,
        )
            -> std::pin::Pin<Box<dyn Future<Output = Result<Reply, CapError>> + Send + 'a>>
        + Send
        + Sync
        + 'static,
{
    Arc::new(move |payload, state, ctx| {
        let handler = Arc::clone(&handler);
        Box::pin(async move {
            let req: Req = trembita_proto::decode(&payload).map_err(CapError::codec)?;
            let fut = handler(req, ctx, state);
            let reply = fut.await?;
            trembita_proto::encode(&reply).map_err(CapError::codec)
        })
    })
}
