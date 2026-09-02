//! Stateful ledger worker — side effects delegated from the job consumer.

use std::sync::atomic::{AtomicUsize, Ordering};

use crafty::actor::{UserActor, actor};

pub static LEDGER_RECORDS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct LedgerErr;

impl std::fmt::Display for LedgerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ledger worker error")
    }
}

impl std::error::Error for LedgerErr {}

pub struct LedgerWorker;

#[actor]
impl UserActor for LedgerWorker {
    type Config = u32;
    type Message = String;
    type Error = LedgerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self)
    }

    fn handle(
        &mut self,
        msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        LEDGER_RECORDS.fetch_add(1, Ordering::SeqCst);
        println!("[ledger] recorded {msg}");
        std::future::ready(Ok(()))
    }
}
