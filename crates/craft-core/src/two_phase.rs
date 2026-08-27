//! Cross-shard two-phase commit plan validation (optional Tier 2 increment).

use std::collections::BTreeSet;

/// Maximum distinct Raft groups in one 2PC transaction.
pub const TWO_PHASE_MAX_GROUPS: usize = 3;
/// Maximum steps in one 2PC transaction.
pub const TWO_PHASE_MAX_STEPS: usize = 16;
/// Maximum encoded command payload per step.
pub const TWO_PHASE_MAX_PAYLOAD: usize = 64 * 1024;

/// One keyed prepare step in a cross-shard 2PC plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwoPhaseStep {
    /// Shard routing key.
    pub key: Vec<u8>,
    /// Application-encoded command staged at prepare time.
    pub command: Vec<u8>,
}

/// Client-coordinated cross-shard 2PC plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwoPhasePlan {
    /// Opaque transaction id shared by all prepare/commit/abort calls.
    pub tx_id: Vec<u8>,
    /// Ordered prepare steps (one per shard write).
    pub steps: Vec<TwoPhaseStep>,
}

/// Why a [`TwoPhasePlan`] fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoPhasePlanError {
    EmptyTxId,
    EmptyPlan,
    TooManySteps,
    PayloadTooLarge {
        /// Zero-based step index.
        step: usize,
    },
    UnroutableKey {
        /// Zero-based step index.
        step: usize,
    },
    TooManyGroups {
        /// Distinct group count.
        groups: usize,
    },
}

impl std::fmt::Display for TwoPhasePlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTxId => f.write_str("transaction id must not be empty"),
            Self::EmptyPlan => f.write_str("plan must contain at least one step"),
            Self::TooManySteps => write!(f, "plan exceeds {TWO_PHASE_MAX_STEPS} steps"),
            Self::PayloadTooLarge { step } => {
                write!(
                    f,
                    "step {step} payload exceeds {TWO_PHASE_MAX_PAYLOAD} bytes"
                )
            }
            Self::UnroutableKey { step } => write!(f, "step {step} key is not routable"),
            Self::TooManyGroups { groups } => write!(
                f,
                "plan spans {groups} groups; maximum is {TWO_PHASE_MAX_GROUPS}"
            ),
        }
    }
}

impl std::error::Error for TwoPhasePlanError {}

/// Validate a cross-shard 2PC plan before issuing prepare calls.
pub fn validate_two_phase_plan(
    plan: &TwoPhasePlan,
    group_for_key: impl Fn(&[u8]) -> Option<u32>,
) -> Result<(), TwoPhasePlanError> {
    if plan.tx_id.is_empty() {
        return Err(TwoPhasePlanError::EmptyTxId);
    }
    if plan.steps.is_empty() {
        return Err(TwoPhasePlanError::EmptyPlan);
    }
    if plan.steps.len() > TWO_PHASE_MAX_STEPS {
        return Err(TwoPhasePlanError::TooManySteps);
    }

    let mut groups = BTreeSet::new();
    for (step, item) in plan.steps.iter().enumerate() {
        if item.command.len() > TWO_PHASE_MAX_PAYLOAD {
            return Err(TwoPhasePlanError::PayloadTooLarge { step });
        }
        let Some(group) = group_for_key(&item.key) else {
            return Err(TwoPhasePlanError::UnroutableKey { step });
        };
        groups.insert(group);
    }
    if groups.len() > TWO_PHASE_MAX_GROUPS {
        return Err(TwoPhasePlanError::TooManyGroups {
            groups: groups.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_two_group_plan() {
        let plan = TwoPhasePlan {
            tx_id: b"tx".to_vec(),
            steps: vec![
                TwoPhaseStep {
                    key: b"a".to_vec(),
                    command: vec![1],
                },
                TwoPhaseStep {
                    key: b"b".to_vec(),
                    command: vec![2],
                },
            ],
        };
        validate_two_phase_plan(&plan, |key| Some(if key == b"a" { 0 } else { 1 })).expect("valid");
    }

    #[test]
    fn rejects_four_groups() {
        let plan = TwoPhasePlan {
            tx_id: b"tx".to_vec(),
            steps: (0..4)
                .map(|i| TwoPhaseStep {
                    key: vec![i as u8],
                    command: vec![1],
                })
                .collect(),
        };
        assert!(matches!(
            validate_two_phase_plan(&plan, |key| Some(key[0] as u32)),
            Err(TwoPhasePlanError::TooManyGroups { groups: 4 })
        ));
    }
}
