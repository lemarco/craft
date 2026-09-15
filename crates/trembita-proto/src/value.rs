//! Domain value objects with validation (wire-compatible via `serde(transparent)` where noted).
//!
//! Design: [domain-value-objects](../../../docs/decisions/domain-value-objects.md).

use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{NodeId, WorkerId};

/// Maximum UTF-8 length for logical resource names (streams, topics, actor groups).
pub const MAX_RESOURCE_NAME_LEN: usize = 256;

/// Maximum byte length for opaque client keys (`dedup_key`, saga/2PC ids).
pub const MAX_OPAQUE_KEY_BYTES: usize = 4096;

/// Validation or construction failure for a value object.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// Name or address was empty after trimming rules.
    #[error("value must not be empty")]
    Empty,
    /// Exceeded a configured maximum length.
    #[error("{field} length {len} exceeds max {max}")]
    TooLong {
        /// Field label for error messages.
        field: &'static str,
        /// Actual length.
        len: usize,
        /// Allowed maximum.
        max: usize,
    },
    /// Disallowed character or format.
    #[error("{0}")]
    InvalidFormat(String),
    /// Numeric invariant violated.
    #[error("{0}")]
    Invariant(String),
}

fn validate_resource_name(field: &'static str, s: &str) -> Result<(), ValueError> {
    if s.is_empty() {
        return Err(ValueError::Empty);
    }
    if s.len() > MAX_RESOURCE_NAME_LEN {
        return Err(ValueError::TooLong {
            field,
            len: s.len(),
            max: MAX_RESOURCE_NAME_LEN,
        });
    }
    if s.chars().any(char::is_control) {
        return Err(ValueError::InvalidFormat(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_opaque_bytes(field: &'static str, bytes: &[u8]) -> Result<(), ValueError> {
    if bytes.is_empty() {
        return Err(ValueError::Empty);
    }
    if bytes.len() > MAX_OPAQUE_KEY_BYTES {
        return Err(ValueError::TooLong {
            field,
            len: bytes.len(),
            max: MAX_OPAQUE_KEY_BYTES,
        });
    }
    Ok(())
}

macro_rules! transparent_string_name {
    ($(#[$meta:meta])* $vis:vis struct $Name:ident;) => {
        $(#[$meta])*
        #[derive(
            Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        $vis struct $Name(String);

        impl $Name {
            /// Construct after validation.
            ///
            /// # Errors
            /// Returns [`ValueError`] when the name is empty, too long, or invalid.
            pub fn try_new(s: impl Into<String>) -> Result<Self, ValueError> {
                let s = s.into();
                validate_resource_name(stringify!($Name), &s)?;
                Ok(Self(s))
            }

            /// Borrow the inner UTF-8 name.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $Name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($Name)).field(&self.0).finish()
            }
        }

        impl fmt::Display for $Name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl Deref for $Name {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $Name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<str> for $Name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl From<$Name> for String {
            fn from(v: $Name) -> Self {
                v.0
            }
        }

        impl FromStr for $Name {
            type Err = ValueError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::try_new(s.to_string())
            }
        }

        impl TryFrom<&str> for $Name {
            type Error = ValueError;
            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

    };
}

transparent_string_name! {
    /// Logical job queue stream (e.g. `"jobs"` or sharded `"jobs~0"`).
    pub struct StreamName;
}
transparent_string_name! {
    /// Durable event topic name.
    pub struct TopicName;
}
transparent_string_name! {
    /// Named subscription within a topic.
    pub struct SubscriptionName;
}
transparent_string_name! {
    /// Actor group / pool name (e.g. `"workers"`).
    pub struct ActorGroupName;
}
transparent_string_name! {
    /// Consistent-hash routing key for actor messages.
    pub struct RoutingKey;
}
transparent_string_name! {
    /// Actor-store or workflow key (UTF-8).
    pub struct StoreKey;
}

impl StreamName {
    /// Physical redb stream for shard `shard` of logical base `base`.
    ///
    /// # Errors
    /// Returns [`ValueError`] when `base` or the combined name is invalid.
    pub fn try_sharded(base: &str, shard: usize) -> Result<Self, ValueError> {
        StreamName::try_new(format!("{base}~{shard}"))
    }

    /// Split `"base~N"` into `(base, Some(N))`; plain names yield `(name, None)`.
    #[must_use]
    pub fn parse_sharded(&self) -> (String, Option<usize>) {
        parse_sharded_stream(self.as_str())
    }
}

/// Parse a queue stream name into base and optional shard index.
#[must_use]
pub fn parse_sharded_stream(name: &str) -> (String, Option<usize>) {
    if let Some((base, shard)) = name.rsplit_once('~')
        && !base.is_empty()
        && let Ok(n) = shard.parse::<usize>()
    {
        return (base.to_string(), Some(n));
    }
    (name.to_string(), None)
}

/// Peer address advertised on join (host:port or DNS name).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AdvertiseAddr(String);

impl AdvertiseAddr {
    /// Construct after validation.
    ///
    /// # Errors
    /// Returns [`ValueError`] when empty or too long.
    pub fn try_new(s: impl Into<String>) -> Result<Self, ValueError> {
        let s = s.into();
        validate_resource_name("advertise_addr", &s)?;
        Ok(Self(s))
    }

    /// Borrow the address string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AdvertiseAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AdvertiseAddr").field(&self.0).finish()
    }
}

impl fmt::Display for AdvertiseAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for AdvertiseAddr {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

/// Wire/protocol version negotiated on join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    /// Current release wire version.
    pub const CURRENT: Self = Self(super::PROTOCOL_VERSION_RAW);

    /// Oldest version this release accepts during rolling upgrades.
    pub const MIN_COMPATIBLE: Self = Self(super::MIN_COMPATIBLE_PROTOCOL_VERSION_RAW);

    /// Whether `other` is in `[MIN_COMPATIBLE..=CURRENT]`.
    #[must_use]
    pub fn is_compatible(self, other: Self) -> bool {
        other.0 >= Self::MIN_COMPATIBLE.0 && other.0 <= Self::CURRENT.0
    }

    /// # Errors
    /// Returns [`ValueError`] when the version is outside the supported band.
    pub fn try_accept(self) -> Result<Self, ValueError> {
        if self.0 >= Self::MIN_COMPATIBLE.0 && self.0 <= Self::CURRENT.0 {
            Ok(self)
        } else {
            Err(ValueError::Invariant(format!(
                "protocol version {} not in [{}, {}]",
                self.0,
                Self::MIN_COMPATIBLE.0,
                Self::CURRENT.0
            )))
        }
    }
}

/// Unix epoch timestamp in milliseconds (wall clock on wire paths).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct UnixMillis(pub u64);

impl UnixMillis {
    /// Sentinel meaning “immediate” on queue/topic wire fields.
    pub const IMMEDIATE: Self = Self(0);
}

/// Scheduled visibility: `None` or zero means “now”.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct NotBefore(pub Option<UnixMillis>);

impl NotBefore {
    /// Effective millis for wire (`0` when absent or immediate).
    #[must_use]
    pub fn to_wire_ms(self) -> u64 {
        self.0.map(|m| m.0).unwrap_or(0)
    }

    /// From wire field (`0` → immediate).
    #[must_use]
    pub fn from_wire_ms(ms: u64) -> Self {
        if ms == 0 {
            Self(None)
        } else {
            Self(Some(UnixMillis(ms)))
        }
    }
}

/// Job enqueue priority (higher leased first).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct JobPriority(pub u8);

/// Delivery attempt ceiling (`0` = unlimited retries).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct MaxAttempts(pub u32);

impl MaxAttempts {
    /// Unlimited redeliveries.
    pub const UNLIMITED: Self = Self(0);

    /// Whether retries are capped.
    #[must_use]
    pub fn is_unlimited(self) -> bool {
        self.0 == 0
    }
}

/// Monotonic job id within a queue stream.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct JobId(pub u64);

/// Lease token for queue or topic delivery.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct LeaseId(pub u64);

/// TTL for actor-store keys in seconds (`0` = no expiry).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct TtlSecs(pub u64);

macro_rules! opaque_key {
    ($(#[$meta:meta])* $vis:vis struct $Name:ident;) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        $vis struct $Name(Vec<u8>);

        impl $Name {
            /// Construct after validation.
            ///
            /// # Errors
            /// Returns [`ValueError`] when empty or too long.
            pub fn try_new(bytes: impl Into<Vec<u8>>) -> Result<Self, ValueError> {
                let bytes = bytes.into();
                validate_opaque_bytes(stringify!($Name), &bytes)?;
                Ok(Self(bytes))
            }

            /// Borrow raw bytes.
            #[must_use]
            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }
        }

        impl fmt::Debug for $Name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($Name))
                    .field(&format_args!("{} bytes", self.0.len()))
                    .finish()
            }
        }

        impl Deref for $Name {
            type Target = [u8];
            fn deref(&self) -> &[u8] {
                &self.0
            }
        }

        impl From<$Name> for Vec<u8> {
            fn from(v: $Name) -> Self {
                v.0
            }
        }
    };
}

opaque_key! {
    /// Client idempotency token for queue enqueue.
    pub struct DedupKey;
}
opaque_key! {
    /// Cross-shard 2PC transaction id.
    pub struct TransactionId;
}
opaque_key! {
    /// Shard routing key for 2PC prepare/abort.
    pub struct RouteKey;
}
opaque_key! {
    /// Saga journal identifier.
    pub struct SagaId;
}

/// Compile-time actor type tag with non-empty validation when constructed explicitly.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActorTypeId(pub String);

impl ActorTypeId {
    /// Construct after validation.
    ///
    /// # Errors
    /// Returns [`ValueError`] when empty or too long.
    pub fn try_new(s: impl Into<String>) -> Result<Self, ValueError> {
        let s = s.into();
        validate_resource_name("actor_type_id", &s)?;
        Ok(Self(s))
    }

    /// Borrow inner tag.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ActorTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ActorTypeId").field(&self.0).finish()
    }
}

/// Logical tick for deterministic Raft timing (not wall clock).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct LogicalTick(pub u64);

impl LogicalTick {
    /// Raw tick count for arithmetic and comparisons.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl From<u64> for LogicalTick {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Decode a [`WorkerId`] from queue/topic wire fields.
#[must_use]
pub fn worker_id_from_wire(node: u64, instance: u32) -> WorkerId {
    WorkerId {
        node: NodeId(node),
        instance,
    }
}

/// Encode [`WorkerId`] to queue/topic wire fields.
#[must_use]
pub fn worker_id_to_wire(worker: WorkerId) -> (u64, u32) {
    (worker.node.0, worker.instance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_validates() {
        assert!(StreamName::try_new("jobs").is_ok());
        assert!(StreamName::try_new("jobs~3").is_ok());
        assert!(StreamName::try_new("").is_err());
    }

    #[test]
    fn parse_sharded_stream_splits() {
        assert_eq!(
            parse_sharded_stream("jobs~2"),
            ("jobs".to_string(), Some(2))
        );
        assert_eq!(parse_sharded_stream("jobs"), ("jobs".to_string(), None));
    }

    #[test]
    fn protocol_version_band() {
        assert!(ProtocolVersion::CURRENT.is_compatible(ProtocolVersion::CURRENT));
        assert!(!ProtocolVersion(0).is_compatible(ProtocolVersion(0)));
    }

    #[test]
    fn opaque_key_rejects_empty() {
        assert!(DedupKey::try_new([] as [u8; 0]).is_err());
        assert!(DedupKey::try_new(b"k").is_ok());
    }

    #[test]
    fn not_before_wire_roundtrip() {
        assert_eq!(NotBefore::from_wire_ms(0).to_wire_ms(), 0);
        assert_eq!(NotBefore::from_wire_ms(42).to_wire_ms(), 42);
    }
}
