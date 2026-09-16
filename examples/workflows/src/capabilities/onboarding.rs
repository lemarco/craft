//! Simulated external onboarding API — one op per saga step type.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

#[derive(Default)]
pub struct OnboardingState {
    store: Mutex<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StepAck;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAccount {
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

#[cap_handler(group = "onboarding", key = "user_id")]
fn run_create_account(
    msg: CreateAccount,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_insert(
        state,
        &format!("account:{}", msg.user_id),
        "active",
        &format!("create_account user={}", msg.user_id),
    )
}

#[cap_handler(group = "onboarding", key = "user_id")]
fn run_compensate_create(
    msg: CompensateCreate,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_remove(
        state,
        &format!("account:{}", msg.user_id),
        &format!("compensate_create user={}", msg.user_id),
    )
}

#[cap_handler(group = "onboarding", key = "user_id")]
fn run_send_welcome(
    msg: SendWelcome,
    state: &mut OnboardingState,
) -> Result<StepAck, CapError> {
    store_insert(
        state,
        &format!("welcome:{}", msg.user_id),
        "sent",
        &format!("send_welcome user={}", msg.user_id),
    )
}

#[cap_handler(group = "onboarding", key = "user_id")]
fn run_compensate_welcome(
    msg: CompensateWelcome,
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
    println!("[onboarding] {line}");
    Ok(StepAck)
}

/// Register onboarding capability group for saga side effects.
#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<OnboardingState>::for_cap::<CreateAccount>().instances(1),
        run_create_account_register,
        run_compensate_create_register,
        run_send_welcome_register,
        run_compensate_welcome_register,
    ))
}
