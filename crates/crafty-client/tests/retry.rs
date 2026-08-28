//! Retry-policy edge cases for [`RemoteClient`]: empty targets, transport
//! failures, per-attempt timeouts, and explicit `NotLeader` hint follow.

use std::collections::VecDeque;
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crafty_client::{Client, ClientError, RemoteClient, RetryPolicy};
use crafty_net::{LocalNetwork, Route, Transport, TransportError, encode_body};
use crafty_proto::{ClientResponse, NodeId};

enum Step {
    Respond(ClientResponse),
    Hang,
}

struct ScriptTransport {
    steps: Mutex<VecDeque<Step>>,
}

impl ScriptTransport {
    fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
        }
    }
}

impl Transport for ScriptTransport {
    fn send(
        &self,
        peer: NodeId,
        _route: Route,
        _body: crafty_net::transport::Body,
    ) -> crafty_net::transport::BoxFuture<
        'static,
        Result<crafty_net::transport::Body, TransportError>,
    > {
        let step = self.steps.lock().expect("poisoned").pop_front();
        Box::pin(async move {
            match step {
                Some(Step::Respond(resp)) => encode_body(&resp).map_err(TransportError::Wire),
                Some(Step::Hang) => {
                    pending::<()>().await;
                    unreachable!()
                }
                None => Err(TransportError::Unreachable(peer)),
            }
        })
    }
}

fn fast_retry(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        attempt_timeout: Duration::from_millis(50),
        backoff: Duration::from_millis(1),
    }
}

#[tokio::test]
async fn client_errors_when_no_targets_configured() {
    let client = RemoteClient::new(Arc::new(LocalNetwork::new()), [] as [NodeId; 0]);
    let err = client.propose(vec![1]).await.unwrap_err();
    assert!(matches!(err, ClientError::NoTargets));
}

#[tokio::test]
async fn client_exhausts_attempts_when_every_target_is_unreachable() {
    let client = RemoteClient::new(Arc::new(LocalNetwork::new()), [NodeId(1), NodeId(2)])
        .with_retry(fast_retry(3));
    let err = client.propose(vec![1]).await.unwrap_err();
    assert!(matches!(err, ClientError::Unreachable { attempts: 3, .. }));
}

#[tokio::test]
async fn client_times_out_when_transport_hangs() {
    let transport = Arc::new(ScriptTransport::new([Step::Hang, Step::Hang]));
    let client = RemoteClient::new(transport, [NodeId(1)]).with_retry(fast_retry(2));
    let err = client.propose(vec![]).await.unwrap_err();
    assert!(matches!(err, ClientError::Timeout { attempts: 2 }));
}

#[tokio::test]
async fn client_follows_not_leader_hint_on_next_attempt() {
    let transport = Arc::new(ScriptTransport::new([
        Step::Respond(ClientResponse::NotLeader {
            leader: Some(NodeId(2)),
        }),
        Step::Respond(ClientResponse::Ok(b"ok".to_vec())),
    ]));
    let client = RemoteClient::new(transport, [NodeId(1), NodeId(2)]).with_retry(fast_retry(2));
    let bytes = client.propose(vec![]).await.expect("propose");
    assert_eq!(bytes, b"ok");
}

#[tokio::test]
async fn client_reports_no_leader_when_hints_are_absent() {
    let transport = Arc::new(ScriptTransport::new([
        Step::Respond(ClientResponse::NotLeader { leader: None }),
        Step::Respond(ClientResponse::NotLeader { leader: None }),
    ]));
    let client = RemoteClient::new(transport, [NodeId(1), NodeId(2)]).with_retry(fast_retry(2));
    let err = client.propose(vec![]).await.unwrap_err();
    assert!(matches!(err, ClientError::NoLeader { attempts: 2 }));
}
