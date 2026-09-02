//! Workflow registration for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use trembita_client::{SagaError, SagaOutcome, SagaPlan};

use super::app::TrembitaApp;

type WorkflowPlanFn = Arc<dyn Fn(&str) -> SagaPlan + Send + Sync>;
type WorkflowRunnerFn = Arc<
    dyn Fn(
            Arc<TrembitaApp>,
            SagaPlan,
        ) -> Pin<Box<dyn Future<Output = Result<SagaOutcome, SagaError>> + Send>>
        + Send
        + Sync,
>;

/// One registered workflow for [`.workflows`](super::app::TrembitaAppBuilder::workflows).
pub struct WorkflowOpts {
    prefix: Option<String>,
    plan: WorkflowPlanFn,
    runner: WorkflowRunnerFn,
}

impl WorkflowOpts {
    /// Register a workflow that handles every saga id (single-workflow apps).
    #[must_use]
    pub fn new<F, R, Fut>(plan: F, runner: R) -> Self
    where
        F: Fn(&str) -> SagaPlan + Send + Sync + 'static,
        R: Fn(Arc<TrembitaApp>, SagaPlan) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SagaOutcome, SagaError>> + Send + 'static,
    {
        Self {
            prefix: None,
            plan: Arc::new(plan),
            runner: Arc::new(move |app, p| Box::pin(runner(app, p))),
        }
    }

    /// Register a workflow selected by saga id prefix (e.g. `"onboard"` matches `onboard-42`).
    #[must_use]
    pub fn named<P, F, R, Fut>(prefix: P, plan: F, runner: R) -> Self
    where
        P: Into<String>,
        F: Fn(&str) -> SagaPlan + Send + Sync + 'static,
        R: Fn(Arc<TrembitaApp>, SagaPlan) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SagaOutcome, SagaError>> + Send + 'static,
    {
        Self {
            prefix: Some(prefix.into()),
            plan: Arc::new(plan),
            runner: Arc::new(move |app, p| Box::pin(runner(app, p))),
        }
    }
}

pub(crate) struct WorkflowRegistration {
    pub prefix: Option<String>,
    pub plan: WorkflowPlanFn,
    pub runner: WorkflowRunnerFn,
}

impl WorkflowOpts {
    pub(crate) fn into_registration(self) -> WorkflowRegistration {
        WorkflowRegistration {
            prefix: self.prefix,
            plan: self.plan,
            runner: self.runner,
        }
    }
}

pub(crate) fn resolve_workflow<'a>(
    workflows: &'a [WorkflowRegistration],
    saga_id: &str,
) -> Result<&'a WorkflowRegistration, SagaError> {
    if workflows.is_empty() {
        return Err(SagaError::Journal(
            trembita_client::SagaJournalError::Backend(
                "workflows require `.workflows([…])`".into(),
            ),
        ));
    }
    if workflows.len() == 1 {
        return Ok(&workflows[0]);
    }
    let mut matches: Vec<_> = workflows
        .iter()
        .filter(|w| {
            w.prefix
                .as_ref()
                .is_some_and(|prefix| saga_id.starts_with(prefix))
        })
        .collect();
    matches.sort_by_key(|w| std::cmp::Reverse(w.prefix.as_ref().map_or(0, String::len)));
    matches.into_iter().next().ok_or_else(|| {
        SagaError::Journal(trembita_client::SagaJournalError::Backend(format!(
            "no workflow registered for saga id {saga_id:?}"
        )))
    })
}
