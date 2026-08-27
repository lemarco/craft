//! `postcard` body framing over HTTP/3 (`docs/protocol.md` §Transport, serialization).
//!
//! HTTP/3 already delimits messages (one request/response body per stream), so
//! "framing" here is thin: bodies are `postcard`-encoded `craft-proto` types
//! tagged with the [`CONTENT_TYPE`], subject to a [`MAX_BODY_BYTES`] guard so a
//! hostile or buggy peer cannot force an unbounded allocation on decode.

use craft_proto::{CodecError, decode, encode};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Content-Type for every `postcard` body.
pub const CONTENT_TYPE: &str = "application/x-postcard";

/// Request header carrying the wire protocol version. Omitted implies `1`
/// (`docs/protocol.md` §Versioning).
pub const PROTOCOL_VERSION_HEADER: &str = "raft-protocol-version";

/// Default QUIC listen port (`docs/protocol.md`; configurable).
pub const DEFAULT_PORT: u16 = 7443;

/// Maximum accepted body size (16 MiB). Larger snapshots stream via chunked
/// `InstallSnapshot` rather than a single frame (`docs/protocol.md` §Connections).
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// An error framing or unframing an HTTP/3 body.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// The body could not be encoded or decoded.
    #[error("codec: {0}")]
    Codec(#[from] CodecError),

    /// The body exceeded [`MAX_BODY_BYTES`].
    #[error("body too large: {size} bytes exceeds limit of {limit}", limit = MAX_BODY_BYTES)]
    BodyTooLarge {
        /// The offending body's size in bytes.
        size: usize,
    },

    /// The request advertised an unsupported `Content-Type`.
    #[error("unsupported content-type: {0:?} (expected {expected})", expected = CONTENT_TYPE)]
    ContentType(String),

    /// The request advertised a wire protocol version this node cannot speak.
    #[error("unsupported protocol version: {got} (this node speaks {expected})")]
    ProtocolVersion {
        /// Version the peer requested.
        got: u32,
        /// Version this node supports.
        expected: u32,
    },
}

/// Encode a value into a `postcard` body, rejecting oversized results.
///
/// # Errors
/// Returns [`WireError::Codec`] on a serialization failure or
/// [`WireError::BodyTooLarge`] if the encoding exceeds [`MAX_BODY_BYTES`].
pub fn encode_body<T: Serialize>(value: &T) -> Result<Vec<u8>, WireError> {
    let bytes = encode(value)?;
    if bytes.len() > MAX_BODY_BYTES {
        return Err(WireError::BodyTooLarge { size: bytes.len() });
    }
    Ok(bytes)
}

/// Decode a value from a `postcard` body, rejecting oversized inputs before
/// touching the bytes.
///
/// # Errors
/// Returns [`WireError::BodyTooLarge`] if `bytes` exceeds [`MAX_BODY_BYTES`], or
/// [`WireError::Codec`] on a deserialization failure.
pub fn decode_body<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireError> {
    if bytes.len() > MAX_BODY_BYTES {
        return Err(WireError::BodyTooLarge { size: bytes.len() });
    }
    Ok(decode(bytes)?)
}

/// Validate a request's `Content-Type` header against [`CONTENT_TYPE`].
///
/// # Errors
/// Returns [`WireError::ContentType`] if it does not match.
pub fn check_content_type(content_type: &str) -> Result<(), WireError> {
    if content_type == CONTENT_TYPE {
        Ok(())
    } else {
        Err(WireError::ContentType(content_type.to_owned()))
    }
}

/// Validate a request's advertised protocol version against the rolling-upgrade
/// compatibility band ([`craft_proto::protocol_version_compatible`]).
///
/// A missing header (`None`) implies version `1` (`docs/protocol.md` §Versioning).
///
/// # Errors
/// Returns [`WireError::ProtocolVersion`] when outside the supported band.
pub fn check_protocol_version(version: Option<u32>) -> Result<(), WireError> {
    let got = version.unwrap_or(1);
    if craft_proto::protocol_version_compatible(got) {
        Ok(())
    } else {
        Err(WireError::ProtocolVersion {
            got,
            expected: craft_proto::PROTOCOL_VERSION,
        })
    }
}
