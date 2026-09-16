//! Onboarding workflow — saga steps invoke capability ops (simulated external side effects).

use std::sync::Arc;

use trembita::TrembitaApp;
use trembita::client::{Client, ClientError, KeyedClient, RemoteClient, SagaError, SagaOutcome, SagaPlan};
use trembita::proto::{decode, encode};
use trembita::{CapVia, Route};
use serde::{Deserialize, Serialize};

use crate::capabilities::onboarding::{
    CompensateCreate, CompensateWelcome, CreateAccount, SendWelcome,
};

/// Wire format in saga step commands (unchanged journal payloads).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnboardingOp {
    /// Create account row in external store (simulated).
    CreateAccount {
        /// User id from saga id.
        user_id: String,
    },
    /// Undo account creation.
    CompensateCreate {
        /// User id from saga id.
        user_id: String,
    },
    /// Enqueue welcome notification (job queue step).
    SendWelcome {
        /// User id from saga id.
        user_id: String,
    },
    /// Undo welcome step.
    CompensateWelcome {
        /// User id from saga id.
        user_id: String,
    },
}

/// Routes saga step commands to onboarding capability ops, then checkpoints in the journal via `()`.
pub struct OnboardingWorkflowClient {
    inner: Arc<RemoteClient>,
    app: Arc<TrembitaApp>,
}

impl OnboardingWorkflowClient {
    /// Build a client that executes onboarding side effects then proposes coordination markers.
    #[must_use]
    pub fn new(app: Arc<TrembitaApp>) -> Self {
        Self {
            inner: app.keyed_client(),
            app,
        }
    }

    async fn apply_op(&self, payload: &[u8]) -> Result<(), ClientError> {
        let op: OnboardingOp = decode(payload).map_err(|e| ClientError::Server(e.to_string()))?;
        let cap_err = |e: trembita::CapError| ClientError::Server(e.to_string());
        match op {
            OnboardingOp::CreateAccount { user_id } => {
                CreateAccount { user_id }
                    .via(self.app.as_ref())
                    .route(Route::Inline)
                    .await
                    .map_err(cap_err)?;
            }
            OnboardingOp::CompensateCreate { user_id } => {
                CompensateCreate { user_id }
                    .via(self.app.as_ref())
                    .route(Route::Inline)
                    .await
                    .map_err(cap_err)?;
            }
            OnboardingOp::SendWelcome { user_id } => {
                SendWelcome { user_id }
                    .via(self.app.as_ref())
                    .route(Route::Inline)
                    .await
                    .map_err(cap_err)?;
            }
            OnboardingOp::CompensateWelcome { user_id } => {
                CompensateWelcome { user_id }
                    .via(self.app.as_ref())
                    .route(Route::Inline)
                    .await
                    .map_err(cap_err)?;
            }
        }
        Ok(())
    }
}

impl Client for OnboardingWorkflowClient {
    async fn propose(&self, payload: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        self.inner.propose(payload).await
    }

    async fn query(&self, payload: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        self.inner.query(payload).await
    }
}

impl KeyedClient for OnboardingWorkflowClient {
    async fn propose_keyed(&self, key: Vec<u8>, payload: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        self.apply_op(&payload).await?;
        let marker = encode(&()).map_err(|e| ClientError::Server(e.to_string()))?;
        self.inner.propose_keyed(key, marker).await
    }

    async fn query_keyed(&self, key: Vec<u8>, payload: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        self.inner.query_keyed(key, payload).await
    }
}

/// Default runner: onboarding client + Meta-Raft journal.
pub async fn run_onboarding_plan(
    app: Arc<TrembitaApp>,
    plan: SagaPlan,
) -> Result<SagaOutcome, SagaError> {
    let client = OnboardingWorkflowClient::new(Arc::clone(&app));
    app.run_workflow(&client, &plan).await
}

/// Resume onboarding workflow after partial progress.
#[allow(dead_code)]
pub async fn resume_onboarding_plan(
    app: Arc<TrembitaApp>,
    plan: SagaPlan,
) -> Result<SagaOutcome, SagaError> {
    let client = OnboardingWorkflowClient::new(Arc::clone(&app));
    app.resume_workflow(&client, &plan).await
}

/// User id extracted from saga id (`onboard-42` → `42`).
#[must_use]
pub fn user_from_saga(saga_id: &str) -> String {
    saga_id
        .strip_prefix("onboard-")
        .unwrap_or(saga_id)
        .to_string()
}

pub fn build_plan(saga_id: &str) -> SagaPlan {
    let user_id = user_from_saga(saga_id);
    let key = user_id.as_bytes().to_vec();
    trembita::WorkflowBuilder::new(saga_id)
        .step(
            "create_account",
            &key,
            encode(&OnboardingOp::CreateAccount {
                user_id: user_id.clone(),
            })
            .expect("encode"),
        )
        .compensate(
            "create_account",
            encode(&OnboardingOp::CompensateCreate {
                user_id: user_id.clone(),
            })
            .expect("encode"),
        )
        .step(
            "send_welcome",
            &key,
            encode(&OnboardingOp::SendWelcome {
                user_id: user_id.clone(),
            })
            .expect("encode"),
        )
        .compensate(
            "send_welcome",
            encode(&OnboardingOp::CompensateWelcome { user_id }).expect("encode"),
        )
        .build()
        .expect("valid workflow")
}

