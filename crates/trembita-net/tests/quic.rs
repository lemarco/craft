//! End-to-end test of the live HTTP/3 transport: a real `QuicServer` and
//! `QuicTransport` exchanging `postcard` RPCs over a mutually-authenticated
//! QUIC connection on loopback (wire-transport, backlog C2).

use std::net::Ipv4Addr;
use std::sync::Arc;

use std::time::{Duration, Instant};
use trembita_net::proto::{
    AppendEntries, AppendEntriesReply, ClientRequest, ClientResponse, LogId, LogIndex, NodeId,
    RaftRpc, RaftRpcReply, Round, Term,
};
use trembita_net::route::Route;
use trembita_net::transport::{Body, BoxFuture};
use trembita_net::wire::{decode_body, encode_body};

use trembita_net::{
    BackoffPolicy, ClusterCa, PeerDirectory, QuicServer, QuicTransport, RequestHandler,
    TransportError, client_config, client_endpoint, send_client_request, send_peer_rpc,
    server_config,
};

/// Answers peer RPCs with a success reply and client requests by echoing the
/// payload — the smallest possible node behind the wire.
struct EchoHandler;

impl RequestHandler for EchoHandler {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
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
                    other => panic!("unexpected rpc {other:?}"),
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
        leader_id: NodeId(2),
        prev_log: LogId::new(Term(0), LogIndex(0)),
        entries: vec![],
        leader_commit: LogIndex(0),
        round: Round(1),
    })
}

/// Stand up an echo server for NodeId(2) and a client transport for NodeId(1),
/// returning the transport with NodeId(2) in its directory.
fn setup() -> QuicTransport {
    let ca = ClusterCa::generate().unwrap();
    let server_id = ca.issue_node(NodeId(2)).unwrap();
    let client_id = ca.issue_node(NodeId(1)).unwrap();

    let server = QuicServer::bind(
        (Ipv4Addr::LOCALHOST, 0).into(),
        server_config(&server_id, ca.root_store().unwrap()).unwrap(),
    )
    .unwrap();
    let server_addr = server.local_addr().unwrap();
    tokio::spawn(server.run(Arc::new(EchoHandler)));

    let endpoint = client_endpoint((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let client_cfg = client_config(&client_id, ca.root_store().unwrap()).unwrap();
    let mut directory = PeerDirectory::new();
    directory.insert(NodeId(2), server_addr);

    QuicTransport::new(endpoint, client_cfg, directory)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_rpc_round_trips_over_mtls_http3() {
    let transport = setup();

    let reply = send_peer_rpc(&transport, NodeId(2), &append_entries(4))
        .await
        .expect("peer rpc should succeed over HTTP/3");
    match reply {
        RaftRpcReply::AppendEntries(r) => {
            assert!(r.success);
            assert_eq!(r.term, Term(4));
            assert_eq!(r.round, Round(1));
        }
        other => panic!("unexpected reply {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_request_round_trips_over_mtls_http3() {
    let transport = setup();

    let resp = send_client_request(
        &transport,
        NodeId(2),
        &ClientRequest::Propose(b"payload".to_vec()),
    )
    .await
    .expect("client request should succeed over HTTP/3");
    assert_eq!(resp, ClientResponse::Ok(b"payload".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_connection_is_reused_across_calls() {
    let transport = setup();

    // Two sequential RPCs; the second reuses the cached QUIC connection.
    for term in [1u64, 2] {
        let reply = send_peer_rpc(&transport, NodeId(2), &append_entries(term))
            .await
            .unwrap();
        assert!(matches!(reply, RaftRpcReply::AppendEntries(r) if r.term == Term(term)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sending_to_a_peer_absent_from_the_directory_is_unreachable() {
    let transport = setup();

    let err = send_peer_rpc(&transport, NodeId(7), &append_entries(1))
        .await
        .unwrap_err();
    assert!(matches!(err, TransportError::Unreachable(NodeId(7))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dial_failure_arms_backoff_and_blocks_immediate_redial() {
    let ca = ClusterCa::generate().unwrap();
    let client_id = ca.issue_node(NodeId(1)).unwrap();
    let endpoint = client_endpoint((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let client_cfg = client_config(&client_id, ca.root_store().unwrap()).unwrap();
    let mut directory = PeerDirectory::new();
    // Nothing listens on this port — the first dial fails and arms backoff.
    directory.insert(NodeId(2), (Ipv4Addr::LOCALHOST, 59999).into());
    let policy = BackoffPolicy {
        base: Duration::from_secs(30),
        max: Duration::from_secs(60),
        factor: 2,
    };
    let transport = QuicTransport::with_backoff(endpoint, client_cfg, directory, policy);

    let _ = send_peer_rpc(&transport, NodeId(2), &append_entries(1))
        .await
        .expect_err("closed port should refuse the first dial");

    let start = Instant::now();
    let err = send_peer_rpc(&transport, NodeId(2), &append_entries(1))
        .await
        .expect_err("backoff should block an immediate redial");
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "backoff short-circuits without another TCP dial"
    );
    assert!(matches!(err, TransportError::Unreachable(NodeId(2))));
}
