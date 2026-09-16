//! Email delivery — queued capability (`Route::Queued` via HTTP `cap_enqueue`).

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use trembita::actor_store::{store_get, store_set};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapVia, OpCtx};

use crate::capabilities::ledger::Record;
use crate::debug;

pub static HANDLED: AtomicUsize = AtomicUsize::new(0);
/// Times the *real* side effect ran. Stays at one per key even under redelivery.
pub static SENT: AtomicUsize = AtomicUsize::new(0);

/// Marker stored under `email:{key}` after side effects succeed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EmailDone;

#[derive(Default)]
pub struct EmailState;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct EmailAck;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverEmail {
    pub text: String,
}

fn job_key(text: &str) -> String {
    text.trim().to_string()
}

fn simulate_redelivery() -> bool {
    env::var("TREMBITA_SIMULATE_REDELIVERY").as_deref() != Ok("0")
}

#[cap_handler(group = "emails", key = "text")]
async fn deliver_email(
    msg: DeliverEmail,
    ctx: OpCtx<'_>,
    _state: &mut EmailState,
) -> Result<EmailAck, CapError> {
    let delivery = HANDLED.fetch_add(1, Ordering::SeqCst) + 1;
    let key = job_key(&msg.text);
    debug::worker_job(0, msg.text.len(), &key);

    let store = ctx
        .app()
        .and_then(trembita::TrembitaApp::actor_state_store)
        .ok_or_else(|| CapError::Handler("data_dir / actor state store required".into()))?;

    let store_key = format!("email:{key}");
    if store_get::<EmailDone>(&*store, &store_key)
        .await
        .map_err(|e| CapError::Handler(e.to_string()))?
        .is_some()
    {
        println!(
            "[worker] delivery #{delivery} — {key}: duplicate, side effect already applied (skipping)"
        );
        return Ok(EmailAck);
    }

    let sent = SENT.fetch_add(1, Ordering::SeqCst) + 1;
    println!("[worker] delivery #{delivery} — {key}: sending email (side effects so far: {sent})");

    let app = ctx
        .app()
        .ok_or_else(|| CapError::Handler("cap handler missing TrembitaApp".into()))?;
    Record {
        key: key.clone(),
    }
    .via(app)
    .fire()
    .await
    .map_err(|e| CapError::Handler(e.to_string()))?;

    store_set(&*store, &store_key, &EmailDone, None)
        .await
        .map_err(|e| CapError::Handler(e.to_string()))?;

    if simulate_redelivery() && delivery == 1 {
        println!("[worker] delivery #{delivery} — {key}: failing before ack (expect redelivery)");
        return Err(CapError::Handler("simulate crash before ack".into()));
    }

    Ok(EmailAck)
}

#[must_use]
pub fn group() -> CapGroup<EmailState> {
    cap_register_chain!(
        CapGroup::<EmailState>::for_cap::<DeliverEmail>()
            .instances(1)
            .default_queue_for::<DeliverEmail>(),
        deliver_email_register,
    )
}
