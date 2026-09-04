use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::oneshot;

use super::ASK_TIMEOUT;
use super::actor::UserActor;
use super::errors::{AskError, SendError};
use super::observer::ComputeTokenHook;
use super::pool::PoolInner;
use super::reply::RpcReplyPort;

async fn await_typed_ask<R>(
    compute_tokens: &ComputeTokenHook,
    rx: oneshot::Receiver<R>,
) -> Result<R, AskError>
where
    R: Send + 'static,
{
    let pool = compute_tokens.lock().unwrap().clone();
    crate::compute_token::with_compute_guard(pool.as_ref(), async {
        match tokio::time::timeout(ASK_TIMEOUT, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(AskError::NoReply),
            Err(_) => Err(AskError::Timeout(ASK_TIMEOUT)),
        }
    })
    .await
}

/// A handle to a single named actor (a group of one). Cheap to clone.
pub struct ActorRef<A: UserActor> {
    pool: Arc<PoolInner<A>>,
    compute_tokens: ComputeTokenHook,
}

impl<A: UserActor> ActorRef<A> {
    pub(super) fn from_pool(pool: Arc<PoolInner<A>>, compute_tokens: ComputeTokenHook) -> Self {
        Self {
            pool,
            compute_tokens,
        }
    }
}

impl<A: UserActor> std::fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("name", &self.pool.name)
            .field("alive", &(self.pool.len() > 0))
            .finish_non_exhaustive()
    }
}

impl<A: UserActor> ActorRef<A> {
    /// Deliver a fire-and-forget message.
    ///
    /// # Errors
    /// Returns [`SendError`] if the actor has stopped.
    pub fn send(&self, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_rr(msg)
    }

    /// Send a request and await its reply. `build` receives an [`RpcReplyPort`]
    /// to embed in the message; the handler replies through it.
    ///
    /// # Errors
    /// Returns [`AskError`] if the message cannot be delivered or the actor
    /// drops the reply without answering.
    pub async fn ask<R, F>(&self, build: F) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnOnce(RpcReplyPort<R>) -> A::Message,
    {
        let (tx, rx) = oneshot::channel();
        self.pool.send_rr(build(RpcReplyPort::local(tx)))?;
        await_typed_ask(&self.compute_tokens, rx).await
    }

    /// Whether the actor still has a live instance.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.pool.len() > 0
    }

    /// The registered name of this actor.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.pool.name
    }

    /// How many supervised restarts this actor has undergone (E14, observability §5).
    /// Always `0` for an unsupervised (`RestartPolicy::Never`) actor.
    #[must_use]
    pub fn restart_count(&self) -> u32 {
        self.pool.restart_count()
    }

    /// Stop the actor and await its task.
    pub async fn stop(&self) {
        self.pool.stop().await;
    }
}

/// A handle to a named pool of actors, routing messages across its instances.
/// Cheap to clone.
#[derive(Clone)]
pub struct PoolRef<A: UserActor> {
    pool: Arc<PoolInner<A>>,
    compute_tokens: ComputeTokenHook,
}

impl<A: UserActor> PoolRef<A> {
    pub(super) fn from_pool(pool: Arc<PoolInner<A>>, compute_tokens: ComputeTokenHook) -> Self {
        Self {
            pool,
            compute_tokens,
        }
    }
}

impl<A: UserActor> std::fmt::Debug for PoolRef<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolRef")
            .field("name", &self.pool.name)
            .field("instances", &self.pool.len())
            .finish_non_exhaustive()
    }
}

impl<A: UserActor> PoolRef<A> {
    /// Deliver a message to the next instance (round-robin).
    ///
    /// # Errors
    /// Returns [`SendError`] if the pool has no live instances.
    pub fn send(&self, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_rr(msg)
    }

    /// Deliver a message to the instance chosen by hashing `key`, so all
    /// messages for the same key reach the same instance (stable within a run;
    /// true consistent hashing across nodes arrives with E8/cluster-routing).
    ///
    /// # Errors
    /// Returns [`SendError`] if the pool has no live instances.
    pub fn send_keyed<K: Hash>(&self, key: &K, msg: A::Message) -> Result<(), SendError> {
        self.pool.send_keyed(crate::ring::hash_key(key), msg)
    }

    /// Ask the next instance (round-robin). See [`ActorRef::ask`].
    ///
    /// # Errors
    /// Returns [`AskError`] if the message cannot be delivered or is dropped.
    pub async fn ask<R, F>(&self, build: F) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnOnce(RpcReplyPort<R>) -> A::Message,
    {
        let (tx, rx) = oneshot::channel();
        self.pool.send_rr(build(RpcReplyPort::local(tx)))?;
        await_typed_ask(&self.compute_tokens, rx).await
    }

    /// Number of live instances.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pool.len()
    }

    /// Whether the pool has no live instances.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pool.len() == 0
    }

    /// The registered name of this pool.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.pool.name
    }

    /// The instance ids currently live in this pool (ascending).
    #[must_use]
    pub fn instance_ids(&self) -> Vec<u32> {
        self.pool.instance_ids()
    }

    /// Stop every instance and await their tasks.
    pub async fn stop(&self) {
        self.pool.stop().await;
    }
}
