//! Onboarding workflow — saga steps call actors (simulated external side effects).

use std::future::Future;
use std::sync::Arc;

use trembita::ActorGroupOpts;
use trembita::TrembitaApp;
use trembita::actor::{UserActor, actor};
use trembita::client::{Client, ClientError, KeyedClient, RemoteClient, SagaOutcome, SagaPlan, SagaError};
use trembita::proto::{decode, encode};
use serde::{Deserialize, Serialize};

/// Side-effect payload encoded in saga step commands (not Raft domain data).
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

#[derive(Debug)]
struct OnboardingErr;
impl std::fmt::Display for OnboardingErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("onboarding worker error")
    }
}
impl std::error::Error for OnboardingErr {}

/// Simulates calls to an external database / notification API.
struct OnboardingWorker {
    store: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
}

#[actor]
impl UserActor for OnboardingWorker {
    type Config = ();
    type Message = OnboardingOp;
    type Error = OnboardingErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self {
            store: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        })
    }

    fn handle(
        &mut self,
        msg: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let line = match &msg {
            OnboardingOp::CreateAccount { user_id } => {
                self.store
                    .lock()
                    .unwrap()
                    .insert(format!("account:{user_id}"), "active".into());
                format!("create_account user={user_id}")
            }
            OnboardingOp::CompensateCreate { user_id } => {
                self.store.lock().unwrap().remove(&format!("account:{user_id}"));
                format!("compensate_create user={user_id}")
            }
            OnboardingOp::SendWelcome { user_id } => {
                self.store
                    .lock()
                    .unwrap()
                    .insert(format!("welcome:{user_id}"), "sent".into());
                format!("send_welcome user={user_id}")
            }
            OnboardingOp::CompensateWelcome { user_id } => {
                self.store.lock().unwrap().remove(&format!("welcome:{user_id}"));
                format!("compensate_welcome user={user_id}")
            }
        };
        crate::debug::onboarding_step(&line);
        println!("[onboarding] {line}");
        std::future::ready(Ok(()))
    }
}

/// Routes saga step commands to onboarding actors, then checkpoints in the journal via `()`.
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
        let bytes = encode(&op).map_err(|e| ClientError::Server(e.to_string()))?;
        self.app
            .cast("onboarding", bytes)
            .await
            .map_err(|e| ClientError::Server(e.to_string()))
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
pub async fn run_onboarding_plan(app: Arc<TrembitaApp>, plan: SagaPlan) -> Result<SagaOutcome, SagaError> {
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
            encode(&OnboardingOp::SendWelcome { user_id: user_id.clone() }).expect("encode"),
        )
        .compensate(
            "send_welcome",
            encode(&OnboardingOp::CompensateWelcome { user_id }).expect("encode"),
        )
        .build()
        .expect("valid workflow")
}

pub fn apply_workers(builder: trembita::TrembitaAppBuilder) -> trembita::TrembitaAppBuilder {
    builder.actors::<OnboardingWorker>("onboarding", ActorGroupOpts::fixed((), 1))
}
