//! R1 — oversized Raft commands are rejected client-side.

use std::sync::Arc;

use trembita_client::{Client, ClientError, KeyedClient, RemoteClient};
use trembita_proto::MAX_RAFT_COMMAND_BYTES;

struct NoopTransport;

impl trembita_net::Transport for NoopTransport {
    fn send(
        &self,
        _peer: trembita_proto::NodeId,
        _route: trembita_net::Route,
        _body: trembita_net::transport::Body,
    ) -> trembita_net::transport::BoxFuture<
        'static,
        Result<trembita_net::transport::Body, trembita_net::TransportError>,
    > {
        Box::pin(async { unreachable!("size check runs before transport") })
    }
}

#[tokio::test]
async fn propose_rejects_oversized_command() {
    let client = RemoteClient::new(Arc::new(NoopTransport), []);
    let payload = vec![0u8; MAX_RAFT_COMMAND_BYTES + 1];
    let err = client.propose(payload).await.unwrap_err();
    assert!(matches!(err, ClientError::CommandTooLarge { .. }));
}

#[tokio::test]
async fn propose_keyed_rejects_oversized_command() {
    let client = RemoteClient::new(Arc::new(NoopTransport), []);
    let payload = vec![0u8; MAX_RAFT_COMMAND_BYTES + 1];
    let err = client
        .propose_keyed(b"k".to_vec(), payload)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::CommandTooLarge { .. }));
}
