//! Typed client wrapper (client-api `TypedClient<M>`, backlog F3).

use std::marker::PhantomData;

use crafty_core::{Command, Query, StateMachine};

use crate::error::ClientError;
use crate::remote::{Client, KeyedClient};

/// A strongly-typed view over any [`Client`], carrying a
/// [`StateMachine`]'s command/query/response types so callers work with real
/// Rust values instead of `postcard` byte vectors.
///
/// ```ignore
/// let typed: TypedClient<RemoteClient, KvMachine> = TypedClient::new(remote);
/// let resp = typed.propose(&KvCommand::Set { key, value }).await?;
/// ```
pub struct TypedClient<C, M> {
    inner: C,
    _marker: PhantomData<fn() -> M>,
}

impl<C, M> TypedClient<C, M> {
    /// Wrap a raw [`Client`] with `M`'s types.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Borrow the underlying raw client.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Unwrap back to the raw client.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C: Client, M: StateMachine> TypedClient<C, M> {
    /// Propose a typed command and decode the typed response.
    ///
    /// # Errors
    /// [`ClientError::Codec`] if the command cannot be encoded or the response
    /// cannot be decoded, otherwise any error from the underlying [`Client`].
    pub async fn propose(&self, command: &M::Command) -> Result<M::Response, ClientError> {
        let payload = Command::to_bytes(command).map_err(|e| ClientError::Codec(e.to_string()))?;
        let bytes = self.inner.propose(payload).await?;
        crafty_proto::decode(&bytes).map_err(|e| ClientError::Codec(e.to_string()))
    }

    /// Run a typed linearizable query and decode the typed response.
    ///
    /// # Errors
    /// [`ClientError::Codec`] if the query cannot be encoded or the response
    /// cannot be decoded, otherwise any error from the underlying [`Client`].
    pub async fn query(&self, query: &M::Query) -> Result<M::Response, ClientError> {
        let payload = Query::to_bytes(query).map_err(|e| ClientError::Codec(e.to_string()))?;
        let bytes = self.inner.query(payload).await?;
        crafty_proto::decode(&bytes).map_err(|e| ClientError::Codec(e.to_string()))
    }
}

impl<C: KeyedClient, M: StateMachine> TypedClient<C, M> {
    /// Propose a typed command to the Raft group that owns `key`.
    ///
    /// # Errors
    /// [`ClientError::Codec`] if the command cannot be encoded or the response
    /// cannot be decoded, otherwise any error from the underlying [`KeyedClient`].
    pub async fn propose_keyed(
        &self,
        key: &[u8],
        command: &M::Command,
    ) -> Result<M::Response, ClientError> {
        let payload = Command::to_bytes(command).map_err(|e| ClientError::Codec(e.to_string()))?;
        let bytes = self.inner.propose_keyed(key.to_vec(), payload).await?;
        crafty_proto::decode(&bytes).map_err(|e| ClientError::Codec(e.to_string()))
    }

    /// Run a typed linearizable query against the Raft group that owns `key`.
    ///
    /// # Errors
    /// [`ClientError::Codec`] if the query cannot be encoded or the response
    /// cannot be decoded, otherwise any error from the underlying [`KeyedClient`].
    pub async fn query_keyed(
        &self,
        key: &[u8],
        query: &M::Query,
    ) -> Result<M::Response, ClientError> {
        let payload = Query::to_bytes(query).map_err(|e| ClientError::Codec(e.to_string()))?;
        let bytes = self.inner.query_keyed(key.to_vec(), payload).await?;
        crafty_proto::decode(&bytes).map_err(|e| ClientError::Codec(e.to_string()))
    }
}
