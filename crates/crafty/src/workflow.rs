//! Fluent builder for [`SagaPlan`](crafty_client::SagaPlan) ([workflows](../../../docs/scenarios/workflows.md)).

use std::collections::HashMap;

use crafty_client::{SagaPlan, SagaStep};

/// Build error from [`WorkflowBuilder::build`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkflowBuildError {
    /// No forward steps were registered.
    #[error("workflow has no steps")]
    Empty,
    /// A `.compensate(id, …)` referenced an unknown step id.
    #[error("unknown step id for compensate: {0}")]
    UnknownStepId(String),
    /// A forward step is missing its compensate command.
    #[error("step {0} has no compensate command")]
    MissingCompensate(String),
}

#[derive(Debug, Clone)]
struct StepDraft {
    key: Vec<u8>,
    command: Vec<u8>,
    compensate: Option<Vec<u8>>,
}

/// Fluent DSL that produces a [`SagaPlan`] for [`run_saga`](crafty_client::run_saga).
#[derive(Debug, Default)]
pub struct WorkflowBuilder {
    saga_id: Vec<u8>,
    steps: Vec<(String, StepDraft)>,
    compensates: HashMap<String, Vec<u8>>,
}

impl WorkflowBuilder {
    /// Start a named workflow (journal key / idempotency scope).
    #[must_use]
    pub fn new(saga_id: impl Into<Vec<u8>>) -> Self {
        Self {
            saga_id: saga_id.into(),
            ..Self::default()
        }
    }

    /// Add a forward step routed by `key` with an encoded command payload.
    #[must_use]
    pub fn step(
        mut self,
        id: impl Into<String>,
        key: impl AsRef<[u8]>,
        command: impl AsRef<[u8]>,
    ) -> Self {
        self.steps.push((
            id.into(),
            StepDraft {
                key: key.as_ref().to_vec(),
                command: command.as_ref().to_vec(),
                compensate: None,
            },
        ));
        self
    }

    /// Attach a compensate payload to a prior step id (same shard as the forward step).
    #[must_use]
    pub fn compensate(mut self, step_id: impl Into<String>, command: impl AsRef<[u8]>) -> Self {
        self.compensates
            .insert(step_id.into(), command.as_ref().to_vec());
        self
    }

    /// Materialize the plan, wiring compensates by step id.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when the workflow is empty or compensates are invalid.
    pub fn build(mut self) -> Result<SagaPlan, WorkflowBuildError> {
        if self.steps.is_empty() {
            return Err(WorkflowBuildError::Empty);
        }
        for (id, draft) in &mut self.steps {
            if let Some(comp) = self.compensates.remove(id) {
                draft.compensate = Some(comp);
            }
        }
        if let Some(unknown) = self.compensates.keys().next() {
            return Err(WorkflowBuildError::UnknownStepId(unknown.clone()));
        }
        let mut plan_steps = Vec::with_capacity(self.steps.len());
        for (id, draft) in self.steps {
            let compensate = draft
                .compensate
                .ok_or_else(|| WorkflowBuildError::MissingCompensate(id.clone()))?;
            plan_steps.push(SagaStep {
                key: draft.key,
                command: draft.command,
                compensate,
            });
        }
        Ok(SagaPlan {
            saga_id: self.saga_id,
            steps: plan_steps,
        })
    }

    /// Derive a stable [`EnqueueOptions::dedup_key`] for a saga step enqueue.
    #[must_use]
    pub fn step_dedup_key(saga_id: impl AsRef<[u8]>, step_id: &str) -> String {
        format!(
            "saga:{}:{step_id}",
            String::from_utf8_lossy(saga_id.as_ref())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_plan_with_compensates() {
        let plan = WorkflowBuilder::new("wf-1")
            .step("a", b"k1", b"cmd1")
            .compensate("a", b"undo1")
            .step("b", b"k2", b"cmd2")
            .compensate("b", b"undo2")
            .build()
            .unwrap();
        assert_eq!(plan.saga_id, b"wf-1");
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].compensate, b"undo1");
    }

    #[test]
    fn step_dedup_key_is_stable() {
        assert_eq!(
            WorkflowBuilder::step_dedup_key("onboard-1", "send_welcome"),
            "saga:onboard-1:send_welcome"
        );
    }
}
