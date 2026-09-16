//! Internal [`UserActor`] host that dispatches registered ops (B-21).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use crate::TrembitaApp;

use trembita_runtime::{MessageDecodeError, UserActor, WireReplyPort};

use super::ctx::OpCtx;
use super::error::CapError;
use super::wire::CapWire;

pub(crate) type OpRunner<S> = Arc<
    dyn for<'a> Fn(
            Vec<u8>,
            &'a mut S,
            OpCtx<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, CapError>> + Send + 'a>>
        + Send
        + Sync,
>;

/// Shared dispatch table for one capability group.
pub struct CapRegistry<S: Send + Default + 'static> {
    ops: HashMap<String, OpRunner<S>>,
}

impl<S: Send + Default + 'static> CapRegistry<S> {
    /// Empty registry — populated at manifest apply time.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            ops: HashMap::new(),
        }
    }

    /// Register a type-erased op runner (internal).
    pub fn register(&mut self, name: &'static str, runner: OpRunner<S>) {
        self.ops.insert(name.to_string(), runner);
    }

    pub(crate) async fn invoke(
        &self,
        op: &str,
        payload: Vec<u8>,
        state: &mut S,
        ctx: OpCtx<'_>,
    ) -> Result<Vec<u8>, CapError> {
        let Some(run) = self.ops.get(op) else {
            return Err(CapError::Handler(format!("unknown op {op}")));
        };
        run(payload, state, ctx).await
    }

    /// Cloneable handle for [`CapHostConfig`].
    #[must_use]
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// Actor configuration — registry shared across instances in the group.
pub struct CapHostConfig<S: Send + Default + 'static> {
    pub(crate) registry: Arc<CapRegistry<S>>,
    pub(crate) app_slot: Arc<OnceLock<Weak<TrembitaApp>>>,
}

impl<S: Send + Default + 'static> Clone for CapHostConfig<S> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            app_slot: Arc::clone(&self.app_slot),
        }
    }
}

pub(crate) enum CapHostMsg {
    Invoke(CapWire),
    Ask { wire: CapWire, reply: WireReplyPort },
}

/// Internal mailbox worker for a capability group.
pub(crate) struct CapHost<S: Send + Default + 'static> {
    state: S,
    registry: Arc<CapRegistry<S>>,
    app_slot: Arc<OnceLock<Weak<TrembitaApp>>>,
}

impl<S: Send + Default + 'static> UserActor for CapHost<S> {
    type Config = CapHostConfig<S>;
    type Message = CapHostMsg;
    type Error = CapError;

    fn start(config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self {
            state: S::default(),
            registry: config.registry,
            app_slot: config.app_slot,
        })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        let app = self.app_slot.get().and_then(|w| w.upgrade());
        let ctx = app
            .as_deref()
            .map(OpCtx::with_app)
            .unwrap_or_else(OpCtx::without_app);
        match msg {
            CapHostMsg::Invoke(wire) => {
                let _ = self
                    .registry
                    .invoke(&wire.op, wire.payload, &mut self.state, ctx)
                    .await?;
            }
            CapHostMsg::Ask { wire, reply } => {
                let out = self
                    .registry
                    .invoke(&wire.op, wire.payload, &mut self.state, ctx)
                    .await?;
                reply.reply_raw(out);
            }
        }
        Ok(())
    }

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        let wire: CapWire = trembita_proto::decode(payload)
            .map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
        Ok(CapHostMsg::Invoke(wire))
    }

    fn decode_ask(
        payload: &[u8],
        reply: WireReplyPort,
    ) -> Result<Self::Message, MessageDecodeError> {
        let wire: CapWire = trembita_proto::decode(payload)
            .map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
        Ok(CapHostMsg::Ask { wire, reply })
    }
}
