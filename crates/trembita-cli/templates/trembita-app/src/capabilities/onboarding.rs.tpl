//! Simulated external onboarding API — one op per saga step type.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use trembita::{CapError, CapGroup, CapManifest, CapOp, OpCtx, Route};

use crate::debug;

#[derive(Default)]
pub struct OnboardingState {
    store: Mutex<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StepAck;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAccount {
    /// User id from saga id.
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompensateCreate {
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendWelcome {
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompensateWelcome {
    pub user_id: String,
}

impl trembita::CapRequest for CreateAccount {
    const GROUP: &'static str = "onboarding";
    const OP: &'static str = "create_account";
    type Reply = StepAck;

    fn cap_key(&self) -> Option<String> {
        Some(self.user_id.clone())
    }
}

impl trembita::CapRequest for CompensateCreate {
    const GROUP: &'static str = "onboarding";
    const OP: &'static str = "compensate_create";
    type Reply = StepAck;

    fn cap_key(&self) -> Option<String> {
        Some(self.user_id.clone())
    }
}

impl trembita::CapRequest for SendWelcome {
    const GROUP: &'static str = "onboarding";
    const OP: &'static str = "send_welcome";
    type Reply = StepAck;

    fn cap_key(&self) -> Option<String> {
        Some(self.user_id.clone())
    }
}

impl trembita::CapRequest for CompensateWelcome {
    const GROUP: &'static str = "onboarding";
    const OP: &'static str = "compensate_welcome";
    type Reply = StepAck;

    fn cap_key(&self) -> Option<String> {
        Some(self.user_id.clone())
    }
}

#[must_use]
pub fn create_account_op() -> CapOp<OnboardingState> {
    CapOp::new("create_account", run_create_account)
        .routes([Route::Inline, Route::InlineFire])
        .key(|m: &CreateAccount| m.user_id.clone())
}

#[must_use]
pub fn compensate_create_op() -> CapOp<OnboardingState> {
    CapOp::new("compensate_create", run_compensate_create)
        .routes([Route::Inline, Route::InlineFire])
        .key(|m: &CompensateCreate| m.user_id.clone())
}

#[must_use]
pub fn send_welcome_op() -> CapOp<OnboardingState> {
    CapOp::new("send_welcome", run_send_welcome)
        .routes([Route::Inline, Route::InlineFire])
        .key(|m: &SendWelcome| m.user_id.clone())
}

#[must_use]
pub fn compensate_welcome_op() -> CapOp<OnboardingState> {
    CapOp::new("compensate_welcome", run_compensate_welcome)
        .routes([Route::Inline, Route::InlineFire])
        .key(|m: &CompensateWelcome| m.user_id.clone())
}

fn run_create_account(
    msg: CreateAccount,
    _ctx: OpCtx<'_>,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_insert(
        state,
        &format!("account:{}", msg.user_id),
        "active",
        &format!("create_account user={}", msg.user_id),
    )
}

fn run_compensate_create(
    msg: CompensateCreate,
    _ctx: OpCtx<'_>,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_remove(
        state,
        &format!("account:{}", msg.user_id),
        &format!("compensate_create user={}", msg.user_id),
    )
}

fn run_send_welcome(
    msg: SendWelcome,
    _ctx: OpCtx<'_>,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_insert(
        state,
        &format!("welcome:{}", msg.user_id),
        "sent",
        &format!("send_welcome user={}", msg.user_id),
    )
}

fn run_compensate_welcome(
    msg: CompensateWelcome,
    _ctx: OpCtx<'_>,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_remove(
        state,
        &format!("welcome:{}", msg.user_id),
        &format!("compensate_welcome user={}", msg.user_id),
    )
}

fn store_insert(
    state: &mut OnboardingState,
    key: &str,
    value: &str,
    line: &str,
) -> Result<StepAck, CapError> {
    state
        .store
        .lock()
        .map_err(|e| CapError::Handler(e.to_string()))?
        .insert(key.to_string(), value.to_string());
    debug::onboarding_step(line);
    println!("[onboarding] {line}");
    Ok(StepAck)
}

fn store_remove(
    state: &mut OnboardingState,
    key: &str,
    line: &str,
) -> Result<StepAck, CapError> {
    state
        .store
        .lock()
        .map_err(|e| CapError::Handler(e.to_string()))?
        .remove(key);
    debug::onboarding_step(line);
    println!("[onboarding] {line}");
    Ok(StepAck)
}

/// Register onboarding capability group for saga side effects.
#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(
        CapGroup::<OnboardingState>::with_state("onboarding")
            .instances(1)
            .op(create_account_op())
            .op(compensate_create_op())
            .op(send_welcome_op())
            .op(compensate_welcome_op()),
    )
}
