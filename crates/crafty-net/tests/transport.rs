//! Tests for the `Transport`/`RequestHandler` ports via the in-memory
//! `LocalNetwork`. These exercise the exact abstraction the QUIC adapter will
//! implement, so the runtime's transport usage is validated without a network.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crafty_net::proto::{
    AppendEntries, AppendEntriesReply, ClientRequest, ClientResponse, LogId, LogIndex, NodeId,
    RaftRpc, RaftRpcReply, Round, Term,
};
use crafty_net::route::Route;
use crafty_net::transport::{Body, BoxFuture};
use crafty_net::wire::{decode_body, encode_body};
use crafty_net::{
    LocalNetwork, RequestHandler, Transport, TransportError, send_client_request, send_peer_rpc,
};

/// A handler that answers peer RPCs with a canned reply and client requests by
/// echoing the payload, counting how many requests it served.
#[derive(Default)]
struct EchoHandler {
    served: AtomicU32,
}

impl RequestHandler for EchoHandler {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        self.served.fetch_add(1, Ordering::SeqCst);
        // Compute the response synchronously, then return an already-ready future
        // (real handlers may await; this proves the plumbing either way).
        let result = (|| match route {
            Route::PeerWire => {
                let rpc: RaftRpc = decode_body(&body)?;
                let reply = match rpc {
                    RaftRpc::AppendEntries(ae) => RaftRpcReply::AppendEntries(AppendEntriesReply {
                        term: ae.term,
                        success: true,
                        conflict_index: None,
                        conflict_term: None,
                        round: ae.round,
                    }),
                    other => panic!("unexpected rpc: {other:?}"),
                };
                Ok(encode_body(&reply)?)
            }
            Route::ClientWire => {
                let req: ClientRequest = decode_body(&body)?;
                let payload = match req {
                    ClientRequest::Propose(p) | ClientRequest::Query(p) => p,
                    ClientRequest::ProposeKeyed { command, .. } => command,
                    ClientRequest::QueryKeyed { query, .. } => query,
                    ClientRequest::ReadIndexConfirm { .. }
                    | ClientRequest::TwoPhasePrepare { .. }
                    | ClientRequest::TwoPhaseCommit { .. }
                    | ClientRequest::TwoPhaseAbort { .. } => Vec::new(),
                };
                Ok(encode_body(&ClientResponse::Ok(payload))?)
            }
            other => Err(TransportError::Io(format!("unhandled route {other:?}"))),
        })();
        Box::pin(async move { result })
    }
}

fn append_entries(term: u64) -> RaftRpc {
    RaftRpc::AppendEntries(AppendEntries {
        term: Term(term),
        leader_id: NodeId(1),
        prev_log: LogId::new(Term(0), LogIndex(0)),
        entries: vec![],
        leader_commit: LogIndex(0),
        round: Round(7),
    })
}

#[tokio::test]
async fn peer_rpc_round_trips_through_the_transport() {
    let net = LocalNetwork::new();
    net.attach(NodeId(2), Arc::new(EchoHandler::default()));

    let reply = send_peer_rpc(&net, NodeId(2), &append_entries(3))
        .await
        .unwrap();

    match reply {
        RaftRpcReply::AppendEntries(r) => {
            assert!(r.success);
            assert_eq!(r.term, Term(3));
            assert_eq!(r.round, Round(7));
        }
        other => panic!("unexpected reply: {other:?}"),
    }
}

#[tokio::test]
async fn client_request_round_trips_through_the_transport() {
    let net = LocalNetwork::new();
    net.attach(NodeId(5), Arc::new(EchoHandler::default()));

    let resp = send_client_request(&net, NodeId(5), &ClientRequest::Propose(b"hello".to_vec()))
        .await
        .unwrap();
    assert_eq!(resp, ClientResponse::Ok(b"hello".to_vec()));
}

#[tokio::test]
async fn sending_to_an_unknown_peer_is_unreachable() {
    let net = LocalNetwork::new();
    let err = send_peer_rpc(&net, NodeId(99), &append_entries(1))
        .await
        .unwrap_err();
    assert!(matches!(err, TransportError::Unreachable(NodeId(99))));
}

#[tokio::test]
async fn detaching_a_node_makes_it_unreachable() {
    let net = LocalNetwork::new();
    net.attach(NodeId(2), Arc::new(EchoHandler::default()));
    assert!(net.is_reachable(NodeId(2)));

    // Reachable before the partition.
    send_peer_rpc(&net, NodeId(2), &append_entries(1))
        .await
        .unwrap();

    assert!(net.detach(NodeId(2)));
    assert!(!net.is_reachable(NodeId(2)));
    assert!(!net.detach(NodeId(2))); // idempotent

    let err = send_peer_rpc(&net, NodeId(2), &append_entries(1))
        .await
        .unwrap_err();
    assert!(matches!(err, TransportError::Unreachable(NodeId(2))));
}

#[tokio::test]
async fn transport_works_behind_an_arc_dyn() {
    let net = LocalNetwork::new();
    net.attach(NodeId(2), Arc::new(EchoHandler::default()));
    let transport: Arc<dyn Transport> = Arc::new(net);

    // The typed helper accepts the trait object transparently.
    let reply = send_peer_rpc(transport.as_ref(), NodeId(2), &append_entries(9))
        .await
        .unwrap();
    assert!(matches!(reply, RaftRpcReply::AppendEntries(r) if r.term == Term(9)));
}

#[tokio::test]
async fn concurrent_sends_all_complete() {
    let net = LocalNetwork::new();
    let handler = Arc::new(EchoHandler::default());
    net.attach(NodeId(2), handler.clone());

    let mut tasks = Vec::new();
    for term in 0..50u64 {
        let net = net.clone();
        tasks.push(tokio::spawn(async move {
            let reply = send_peer_rpc(&net, NodeId(2), &append_entries(term))
                .await
                .unwrap();
            matches!(reply, RaftRpcReply::AppendEntries(r) if r.term == Term(term))
        }));
    }

    for task in tasks {
        assert!(task.await.unwrap());
    }
    assert_eq!(handler.served.load(Ordering::SeqCst), 50);
}
