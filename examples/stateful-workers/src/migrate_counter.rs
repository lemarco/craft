//! Migratable counter actor used by local and QUIC migration demos.

use crafty::actor::{ConfigCodecError, MigrationError, UserActor, actor};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum CounterMsg {
    /// Increment the counter.
    Inc,
}

pub struct StatefulCounter {
    pub(crate) count: u64,
}

#[derive(Debug)]
pub struct CounterErr;
impl std::fmt::Display for CounterErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counter error")
    }
}
impl std::error::Error for CounterErr {}

#[actor(migratable)]
impl UserActor for StatefulCounter {
    type Config = u64;
    type Message = CounterMsg;
    type Error = CounterErr;

    fn start(initial: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self { count: initial })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        if matches!(msg, CounterMsg::Inc) {
            self.count += 1;
            println!("[counter] → {}", self.count);
        }
        Ok(())
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        crafty::proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        crafty::proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn migration_snapshot(&self) -> Result<Vec<u8>, MigrationError> {
        crafty::proto::encode(&self.count).map_err(MigrationError::new)
    }

    fn restore_migration(&mut self, snapshot: &[u8]) -> Result<(), MigrationError> {
        self.count = crafty::proto::decode(snapshot).map_err(MigrationError::new)?;
        Ok(())
    }
}
