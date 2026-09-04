use std::sync::Arc;

use trembita_client::{SagaError, SagaOutcome, SagaPlan};

use super::runtime::TrembitaApp;

/// Run a workflow using the default keyed client and Meta-Raft journal.
///
/// Pass as the runner to [`.workflows`](super::TrembitaAppBuilder::workflows) when no custom client is needed.
///
/// # Errors
/// Same as [`TrembitaApp::run_workflow`].
pub async fn journal_workflow(
    app: Arc<TrembitaApp>,
    plan: SagaPlan,
) -> Result<SagaOutcome, SagaError> {
    let client = app.keyed_client();
    app.run_workflow(client.as_ref(), &plan).await
}
