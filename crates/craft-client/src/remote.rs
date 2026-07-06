//! The [`Client`] trait and the HTTP/3 [`RemoteClient`] (ADR 002 L2, ADR 003).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use craft_net::send_client_request;
use craft_net::transport::Transport;
use craft_proto::{ClientRequest, ClientResponse, NodeId};

use crate::error::ClientError;

/// A cluster client: submit an application-encoded write (`propose`) or
/// linearizable read (`query`) and get the application-encoded response back.
///
/// The raw-bytes layer shared by the remote HTTP/3 client and any in-process
/// adapter; [`TypedClient`](crate::TypedClient) wraps it with a
/// [`StateMachine`](craft_core::StateMachine)'s command/query/response types.
pub trait Client {
    /// Submit a write. `payload` is the application-encoded command; the
    /// returned bytes are the application-encoded response.
    fn propose(
        &self,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;

    /// Submit a linearizable read (ReadIndex, ADR 005). `payload` is the
    /// application-encoded query; the returned bytes are the encoded response.
    fn query(&self, payload: Vec<u8>) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send;
}

/// How a [`RemoteClient`] retries across nodes and elections (backlog F4).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total number of send attempts before giving up (each attempt may target
    /// a different node).
    pub max_attempts: u32,
    /// Deadline for a single attempt (the follower→leader forward hop of ADR
    /// 003 happens inside this window on the server side).
    pub attempt_timeout: Duration,
    /// Delay between attempts, giving an in-progress election time to settle.
    pub backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            attempt_timeout: Duration::from_secs(5),
            backoff: Duration::from_millis(100),
        }
    }
}

/// A remote client that talks to a craft cluster over any [`Transport`]
/// (live QUIC/HTTP/3 in production, the in-memory `LocalNetwork` in tests).
///
/// A client may contact **any** node: a follower transparently forwards to the
/// leader server-side (ADR 003), so a single reachable node is enough. The
/// client is nonetheless configured with several `targets` and a
/// [`RetryPolicy`] so it survives a node being down or an election in flight —
/// it rotates across targets, and follows a `NotLeader` hint straight to the
/// named leader when one is returned.
pub struct RemoteClient {
    transport: Arc<dyn Transport>,
    targets: Vec<NodeId>,
    retry: RetryPolicy,
    cursor: AtomicUsize,
}

impl RemoteClient {
    /// Build a client over `transport` that contacts `targets` (seed node ids;
    /// the transport resolves each to an address). Uses the default
    /// [`RetryPolicy`].
    #[must_use]
    pub fn new(transport: Arc<dyn Transport>, targets: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            transport,
            targets: targets.into_iter().collect(),
            retry: RetryPolicy::default(),
            cursor: AtomicUsize::new(0),
        }
    }

    /// Override the [`RetryPolicy`].
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// The seed nodes this client rotates across.
    #[must_use]
    pub fn targets(&self) -> &[NodeId] {
        &self.targets
    }

    /// Send one request with failover + leader-follow retry.
    async fn call(&self, request: ClientRequest) -> Result<Vec<u8>, ClientError> {
        let n = self.targets.len();
        if n == 0 {
            return Err(ClientError::NoTargets);
        }
        let attempts = self.retry.max_attempts.max(1);
        // Start from a rotating offset so load spreads across nodes and a
        // downed seed does not trap every client on the same first hop.
        let mut idx = self.cursor.fetch_add(1, Ordering::Relaxed) % n;
        let mut last = ClientError::NoLeader { attempts };

        for attempt in 0..attempts {
            let target = self.targets[idx % n];
            let send = send_client_request(&*self.transport, target, &request);
            match tokio::time::timeout(self.retry.attempt_timeout, send).await {
                Ok(Ok(ClientResponse::Ok(bytes))) => return Ok(bytes),
                Ok(Ok(ClientResponse::NotLeader { leader })) => {
                    last = ClientError::NoLeader { attempts };
                    // Follow a concrete leader hint straight to that node;
                    // otherwise rotate to the next target.
                    idx = leader
                        .and_then(|l| self.targets.iter().position(|t| *t == l))
                        .unwrap_or(idx + 1);
                }
                Ok(Ok(ClientResponse::Error(msg))) => {
                    // The runtime returns `Error` for "no leader elected" during
                    // an election as well as for definitive failures; retry, and
                    // surface the last message if every attempt fails.
                    last = ClientError::Server(msg);
                    idx += 1;
                }
                Ok(Err(e)) => {
                    last = ClientError::Unreachable {
                        attempts,
                        last: e.to_string(),
                    };
                    idx += 1;
                }
                Err(_elapsed) => {
                    last = ClientError::Timeout { attempts };
                    idx += 1;
                }
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(self.retry.backoff).await;
            }
        }
        Err(last)
    }
}

impl Client for RemoteClient {
    fn propose(
        &self,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send {
        self.call(ClientRequest::Propose(payload))
    }

    fn query(&self, payload: Vec<u8>) -> impl Future<Output = Result<Vec<u8>, ClientError>> + Send {
        self.call(ClientRequest::Query(payload))
    }
}
