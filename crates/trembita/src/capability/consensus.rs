//! Linearizable Raft and cross-shard workflow helpers from capability handlers.

use std::sync::Arc;

use trembita_client::{Client, KeyedClient, SagaOutcome, SagaPlan};

use crate::TrembitaApp;

use super::ctx::OpCtx;
use super::error::CapError;

impl OpCtx<'_> {
    /// Running app handle for cluster APIs (propose, query, saga).
    ///
    /// # Errors
    /// [`CapError::MissingOption`] when the handler was invoked without an app (unit tests).
    pub fn require_app(&self) -> Result<&TrembitaApp, CapError> {
        self.app().ok_or_else(|| CapError::MissingOption {
            detail: "OpCtx::require_app needs in-process TrembitaApp (inline/queued bridge)".into(),
        })
    }

    /// Keyed Raft client for linearizable reads and shard-aware writes.
    ///
    /// # Errors
    /// Same as [`Self::require_app`].
    pub fn keyed_client(&self) -> Result<Arc<trembita_client::RemoteClient>, CapError> {
        Ok(self.require_app()?.keyed_client())
    }

    /// Linearizable read on the default Raft group ([`Client::query`](trembita_client::Client::query)).
    ///
    /// # Errors
    /// [`CapError::Consensus`] when the query fails.
    pub async fn query_linearizable(&self, query: &[u8]) -> Result<Vec<u8>, CapError> {
        let client = self.keyed_client()?;
        client
            .query(query.to_vec())
            .await
            .map_err(CapError::consensus)
    }

    /// Linearizable read on the shard that owns `key`.
    ///
    /// # Errors
    /// [`CapError::Consensus`] when the query fails.
    pub async fn query_keyed_linearizable(
        &self,
        key: &[u8],
        query: &[u8],
    ) -> Result<Vec<u8>, CapError> {
        let client = self.keyed_client()?;
        client
            .query_keyed(key.to_vec(), query.to_vec())
            .await
            .map_err(CapError::consensus)
    }

    /// Replicated write on the shard that owns `key` (keep commands small — R1).
    ///
    /// # Errors
    /// [`CapError::Consensus`] when propose fails (including command too large).
    pub async fn propose_keyed(&self, key: &[u8], command: &[u8]) -> Result<Vec<u8>, CapError> {
        let client = self.keyed_client()?;
        client
            .propose_keyed(key.to_vec(), command.to_vec())
            .await
            .map_err(CapError::consensus)
    }

    /// Run a keyed cross-shard saga (all steps commit or compensators run).
    ///
    /// # Errors
    /// [`CapError::Saga`] when coordination fails.
    pub async fn run_keyed_saga(&self, plan: &SagaPlan) -> Result<SagaOutcome, CapError> {
        let app = self.require_app()?;
        let client = app.keyed_client();
        app.run_workflow(client.as_ref(), plan)
            .await
            .map_err(CapError::saga)
    }

    /// Resume a keyed saga after crash or partial progress.
    ///
    /// # Errors
    /// [`CapError::Saga`] when resume fails.
    pub async fn resume_keyed_saga(&self, plan: &SagaPlan) -> Result<SagaOutcome, CapError> {
        let app = self.require_app()?;
        let client = app.keyed_client();
        app.resume_workflow(client.as_ref(), plan)
            .await
            .map_err(CapError::saga)
    }

    /// Cross-shard two-phase commit (≤3 groups; opt-in on cluster builder).
    ///
    /// # Errors
    /// [`CapError::Consensus`] when 2PC fails.
    pub async fn run_cross_shard_2pc(
        &self,
        plan: &trembita_core::TwoPhasePlan,
    ) -> Result<Vec<Vec<u8>>, CapError> {
        let app = self.require_app()?;
        let client = app.keyed_client();
        app.cluster()
            .run_keyed_2pc(client.as_ref(), plan)
            .await
            .map_err(CapError::consensus)
    }
}

#[cfg(test)]
mod tests {
    use super::CapError;

    #[test]
    fn not_linearizable_points_to_raft_query() {
        let err = CapError::not_linearizable_actor_read("get_balance");
        assert!(err.to_string().contains("query_keyed_linearizable"));
    }
}
