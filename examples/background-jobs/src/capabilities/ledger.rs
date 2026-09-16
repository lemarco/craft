//! Durable accounting — fire-and-forget from the email consumer.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

pub static LEDGER_RECORDS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct LedgerState;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RecordAck;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
}

#[cap_handler(group = "ledger")]
fn record(msg: Record, _state: &mut LedgerState) -> Result<RecordAck, CapError> {
    LEDGER_RECORDS.fetch_add(1, Ordering::SeqCst);
    println!("[ledger] recorded {}", msg.key);
    Ok(RecordAck)
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<LedgerState>::for_cap::<Record>().instances(1),
        record_register,
    ))
}
