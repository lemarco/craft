//! Client-side error type.

/// Why a client request could not be completed.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ClientError {
    /// The client was created with no target nodes to contact.
    #[error("no target nodes configured")]
    NoTargets,
    /// Every attempt exhausted without reaching a leader (an election is in
    /// progress, or every contacted node forwarded but no leader answered).
    #[error("no leader available after {attempts} attempt(s)")]
    NoLeader {
        /// How many attempts were made.
        attempts: u32,
    },
    /// Every attempt exhausted without a reachable node.
    #[error("all {attempts} attempt(s) failed; last transport error: {last}")]
    Unreachable {
        /// How many attempts were made.
        attempts: u32,
        /// The last transport error observed.
        last: String,
    },
    /// A request attempt exceeded its per-attempt deadline on every try.
    #[error("request timed out after {attempts} attempt(s)")]
    Timeout {
        /// How many attempts were made.
        attempts: u32,
    },
    /// The cluster reported an application/processing error (returned verbatim
    /// from the leader). Not retried — it is a definitive answer.
    #[error("cluster error: {0}")]
    Server(String),
    /// A request/response body could not be encoded or decoded.
    #[error("codec error: {0}")]
    Codec(String),
}
