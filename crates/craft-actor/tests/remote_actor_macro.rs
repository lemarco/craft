//! Tests for the `#[craft_actor::remote_actor]` attribute (backlog D2): it
//! should fill in the `postcard` wire codecs on a `UserActor` impl, honour the
//! `migratable` flag, and leave any hand-written codec method untouched.

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use craft_actor::{MessageDecodeError, UserActor, remote_actor};
use serde::{Deserialize, Serialize};

/// Read `MIGRATABLE` behind a function boundary so the assertions on it are not
/// flagged as constant (`clippy::assertions_on_constants`).
fn migratable<A: UserActor>() -> bool {
    A::MIGRATABLE
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Cfg {
    start: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Msg {
    Inc,
    Add(u64),
}

#[derive(Debug, thiserror::Error)]
#[error("counter error")]
struct CounterError;

struct Counter {
    n: u64,
}

#[remote_actor]
impl UserActor for Counter {
    type Config = Cfg;
    type Message = Msg;
    type Error = CounterError;

    fn start(config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Counter { n: config.start })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            Msg::Inc => self.n += 1,
            Msg::Add(k) => self.n += k,
        }
        Ok(())
    }
}

#[test]
fn generated_config_codec_round_trips() {
    let cfg = Cfg { start: 7 };
    let bytes = Counter::encode_config(&cfg).expect("encode");
    let back = Counter::decode_config(&bytes).expect("decode");
    assert_eq!(back, cfg);
}

#[test]
fn generated_message_decode_round_trips() {
    let bytes = craft_actor::craft_proto::encode(&Msg::Add(5)).unwrap();
    let msg = Counter::decode_message(&bytes).expect("decode");
    assert_eq!(msg, Msg::Add(5));
}

#[test]
fn invalid_message_bytes_are_a_decode_error_not_a_panic() {
    // An empty payload cannot decode into the `Msg` enum discriminant.
    let err = Counter::decode_message(&[]);
    assert!(matches!(err, Err(MessageDecodeError::Decode(_))), "{err:?}");
}

#[test]
fn actor_is_not_migratable_by_default() {
    assert!(!migratable::<Counter>());
}

// ---------------------------------------------------------------------------
// `migratable` flag
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize)]
struct SessionCfg;

#[derive(Serialize, Deserialize)]
struct SessionMsg;

struct Session;

#[remote_actor(migratable)]
impl UserActor for Session {
    type Config = SessionCfg;
    type Message = SessionMsg;
    type Error = CounterError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Session)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn migratable_flag_sets_the_const() {
    assert!(migratable::<Session>());
}

// ---------------------------------------------------------------------------
// Hand-written codec method is preserved (not duplicated)
// ---------------------------------------------------------------------------

struct LocalOnly {
    n: u64,
}

#[remote_actor]
impl UserActor for LocalOnly {
    type Config = Cfg;
    type Message = Msg;
    type Error = CounterError;

    fn start(config: Self::Config) -> Result<Self, Self::Error> {
        Ok(LocalOnly { n: config.start })
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        self.n += 1;
        Ok(())
    }

    // Deliberately keep messages local-only; the attribute must not clobber it.
    fn decode_message(_payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        Err(MessageDecodeError::NotAddressable)
    }
}

#[test]
fn hand_written_codec_is_not_overwritten() {
    assert!(matches!(
        LocalOnly::decode_message(&[]),
        Err(MessageDecodeError::NotAddressable)
    ));
    // ...but the config codec was still generated for us.
    assert!(LocalOnly::encode_config(&Cfg { start: 1 }).is_ok());
}
