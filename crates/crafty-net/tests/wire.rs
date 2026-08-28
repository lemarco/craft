//! Contract tests for the transport-agnostic `crafty-net` core: the route table,
//! `postcard` body framing, and the peer directory.

use crafty_net::proto::{
    AppendEntries, ClientRequest, ClientResponse, LogId, LogIndex, NodeId, RaftRpc, RaftRpcReply,
    Round, Term,
};
use crafty_net::route::{self, Route, TrafficClass};
use crafty_net::wire::{
    self, CONTENT_TYPE, MAX_BODY_BYTES, check_content_type, check_protocol_version, decode_body,
    encode_body,
};
use crafty_net::{PeerDirectory, WireError};

// --- Routes -----------------------------------------------------------------

#[test]
fn every_route_round_trips_through_its_path() {
    for route in Route::ALL {
        assert_eq!(Route::from_path(route.path()), Some(route));
        assert_eq!(route.method(), "POST");
        assert!(route.path().starts_with(route::API_PREFIX));
    }
}

#[test]
fn unknown_paths_do_not_resolve() {
    assert_eq!(Route::from_path("/raft/v1/nope"), None);
    assert_eq!(Route::from_path("/peer/wire"), None); // missing version prefix
    assert_eq!(Route::from_path(""), None);
}

#[test]
fn traffic_classes_isolate_peer_consensus_from_the_rest() {
    assert_eq!(Route::PeerWire.traffic_class(), TrafficClass::Peer);
    assert_eq!(Route::ClientWire.traffic_class(), TrafficClass::Client);
    assert_eq!(Route::ClusterJoin.traffic_class(), TrafficClass::Cluster);
    for actor in [
        Route::ActorDeliver,
        Route::ActorSpawn,
        Route::ActorMigrate,
        Route::ActorRegister,
        Route::QueueEnqueue,
        Route::QueueLease,
        Route::QueueAck,
        Route::QueueNack,
        Route::QueueMetrics,
        Route::QueueReplicate,
    ] {
        assert_eq!(actor.traffic_class(), TrafficClass::Actor);
    }
    // Peer consensus never shares a class with any other route.
    for route in Route::ALL {
        if route != Route::PeerWire {
            assert_ne!(route.traffic_class(), TrafficClass::Peer);
        }
    }
}

#[test]
fn route_paths_are_unique() {
    let mut paths: Vec<&str> = Route::ALL.iter().map(|r| r.path()).collect();
    paths.sort_unstable();
    let count = paths.len();
    paths.dedup();
    assert_eq!(paths.len(), count, "route paths must be unique");
}

// --- Wire framing -----------------------------------------------------------

fn sample_append_entries() -> RaftRpc {
    RaftRpc::AppendEntries(AppendEntries {
        term: Term(4),
        leader_id: NodeId(1),
        prev_log: LogId::new(Term(3), LogIndex(7)),
        entries: vec![],
        leader_commit: LogIndex(7),
        round: Round(2),
    })
}

#[test]
fn peer_rpc_body_round_trips() {
    let rpc = sample_append_entries();
    let body = encode_body(&rpc).unwrap();
    let decoded: RaftRpc = decode_body(&body).unwrap();
    assert_eq!(decoded, rpc);
}

#[test]
fn peer_reply_body_round_trips() {
    let reply = RaftRpcReply::AppendEntries(crafty_net::proto::AppendEntriesReply {
        term: Term(4),
        success: true,
        conflict_index: None,
        conflict_term: None,
        round: Round(2),
    });
    let body = encode_body(&reply).unwrap();
    assert_eq!(decode_body::<RaftRpcReply>(&body).unwrap(), reply);
}

#[test]
fn client_request_and_response_bodies_round_trip() {
    let req = ClientRequest::Propose(b"set x=1".to_vec());
    let body = encode_body(&req).unwrap();
    assert_eq!(decode_body::<ClientRequest>(&body).unwrap(), req);

    let resp = ClientResponse::Ok(b"done".to_vec());
    let body = encode_body(&resp).unwrap();
    assert_eq!(decode_body::<ClientResponse>(&body).unwrap(), resp);
}

#[test]
fn decode_rejects_oversized_bodies_before_parsing() {
    let oversized = vec![0u8; MAX_BODY_BYTES + 1];
    let err = decode_body::<RaftRpc>(&oversized).unwrap_err();
    assert!(matches!(err, WireError::BodyTooLarge { size } if size == MAX_BODY_BYTES + 1));
}

#[test]
fn a_body_exactly_at_the_limit_is_not_rejected_for_size() {
    // The size guard must fire on `> MAX`, never on `== MAX`, so a limit-sized
    // body is never a `BodyTooLarge` (whatever the decode result turns out to
    // be). Only `MAX + 1` should trip it.
    let at_limit = vec![0u8; MAX_BODY_BYTES];
    assert!(!matches!(
        decode_body::<RaftRpc>(&at_limit),
        Err(WireError::BodyTooLarge { .. })
    ));
    assert!(matches!(
        decode_body::<RaftRpc>(&vec![0u8; MAX_BODY_BYTES + 1]),
        Err(WireError::BodyTooLarge { .. })
    ));
}

#[test]
fn content_type_is_validated() {
    assert!(check_content_type(CONTENT_TYPE).is_ok());
    let err = check_content_type("application/json").unwrap_err();
    assert!(matches!(err, WireError::ContentType(ct) if ct == "application/json"));
}

#[test]
fn protocol_version_missing_header_defaults_to_one() {
    assert!(check_protocol_version(None).is_ok());
    assert!(check_protocol_version(Some(crafty_net::proto::PROTOCOL_VERSION)).is_ok());
    let err = check_protocol_version(Some(999)).unwrap_err();
    assert!(matches!(err, WireError::ProtocolVersion { got: 999, .. }));
}

#[test]
fn default_port_matches_the_spec() {
    assert_eq!(wire::DEFAULT_PORT, 7443);
}

// --- Peer directory ---------------------------------------------------------

#[test]
fn peer_directory_insert_lookup_remove() {
    let mut dir = PeerDirectory::new();
    assert!(dir.is_empty());

    let a: std::net::SocketAddr = "10.0.0.1:7443".parse().unwrap();
    let b: std::net::SocketAddr = "10.0.0.2:7443".parse().unwrap();

    assert_eq!(dir.insert(NodeId(1), a), None);
    assert_eq!(dir.insert(NodeId(2), b), None);
    assert_eq!(dir.len(), 2);
    assert_eq!(dir.addr(NodeId(1)), Some(a));
    assert!(dir.contains(NodeId(2)));

    // Re-insert returns the previous address.
    let a2: std::net::SocketAddr = "10.0.0.9:7443".parse().unwrap();
    assert_eq!(dir.insert(NodeId(1), a2), Some(a));
    assert_eq!(dir.addr(NodeId(1)), Some(a2));

    assert_eq!(dir.remove(NodeId(1)), Some(a2));
    assert!(!dir.contains(NodeId(1)));
    assert_eq!(dir.node_ids(), vec![NodeId(2)]);
}

#[test]
fn peer_directory_builds_route_urls() {
    let mut dir = PeerDirectory::new();
    dir.insert(NodeId(7), "192.168.1.5:7443".parse().unwrap());

    assert_eq!(
        dir.url(NodeId(7), Route::PeerWire).as_deref(),
        Some("https://192.168.1.5:7443/raft/v1/peer/wire")
    );
    assert_eq!(dir.url(NodeId(404), Route::PeerWire), None);
}

#[test]
fn peer_directory_brackets_ipv6_urls() {
    let mut dir = PeerDirectory::new();
    dir.insert(NodeId(1), "[::1]:7443".parse().unwrap());
    assert_eq!(
        dir.url(NodeId(1), Route::ClientWire).as_deref(),
        Some("https://[::1]:7443/raft/v1/client/wire")
    );
}

#[test]
fn peer_directory_collects_from_iterator_in_order() {
    let dir: PeerDirectory = [
        (NodeId(3), "10.0.0.3:7443".parse().unwrap()),
        (NodeId(1), "10.0.0.1:7443".parse().unwrap()),
        (NodeId(2), "10.0.0.2:7443".parse().unwrap()),
    ]
    .into_iter()
    .collect();
    assert_eq!(dir.node_ids(), vec![NodeId(1), NodeId(2), NodeId(3)]);
    let collected: Vec<NodeId> = dir.iter().map(|(id, _)| id).collect();
    assert_eq!(collected, vec![NodeId(1), NodeId(2), NodeId(3)]);
}
