use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use trembita_net::{QuicTransport, fetch_peers, send_join_request};
use trembita_proto::{
    JoinRejection, JoinRequest, JoinResponse, JoinRole, Membership, NodeId, PROTOCOL_VERSION,
};

use crate::discovery::Seed;

use super::StartError;

/// Initial voter set before a dynamic join commits (excludes `node_id`).
pub(crate) fn consensus_bootstrap_voters(
    members: &[NodeId],
    node_id: NodeId,
    dynamic_join: bool,
) -> Vec<NodeId> {
    if !dynamic_join {
        return members.to_vec();
    }
    let live: Vec<_> = members
        .iter()
        .copied()
        .filter(|id| *id != node_id)
        .collect();
    if live.is_empty() { vec![node_id] } else { live }
}

/// How long to keep retrying each phase of a dynamic join before giving up.
const JOIN_ATTEMPTS: u32 = 40;
/// Delay between join attempts (≈`JOIN_ATTEMPTS × JOIN_BACKOFF` total budget).
const JOIN_BACKOFF: Duration = Duration::from_millis(250);

/// Drive a dynamic join against a **seed set** (discovery, join-rpc): learn the
/// cluster's peer addresses from whichever seed answers first, then submit a
/// `/cluster/join` (forwarded to the leader) until it commits, the cluster
/// refuses it, or the retry budget is exhausted. Each attempt rotates through
/// every seed so one dead/relocated seed cannot block the join.
pub(crate) async fn join_cluster(
    quic: &Arc<QuicTransport>,
    node_id: NodeId,
    seeds: &[Seed],
    advertise: SocketAddr,
    role: JoinRole,
) -> Result<(), StartError> {
    debug_assert!(!seeds.is_empty(), "join_cluster requires at least one seed");

    // Phase 1: pull the peer-address book from any reachable seed so we can
    // reach the leader (and every member) directly once added.
    let mut booked = false;
    let mut last_err = String::from("no seeds");
    'book: for attempt in 0..JOIN_ATTEMPTS {
        for seed in seeds {
            match fetch_peers(&**quic, seed.node_id).await {
                Ok(book) => {
                    for entry in book.entries {
                        if let Ok(addr) = entry.addr.parse::<SocketAddr>() {
                            quic.learn_peer(entry.node, addr);
                        }
                    }
                    booked = true;
                    break 'book;
                }
                Err(e) => last_err = e.to_string(),
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    if !booked {
        return Err(StartError::Join(format!(
            "no seed reachable to bootstrap discovery: {last_err}"
        )));
    }

    // Phase 2: ask to join. Any seed forwards to the leader on our behalf, so a
    // `Redirect` means "no leader yet" — retry against the next seed.
    let request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: Some(node_id),
        advertise_addr: advertise.to_string(),
        role,
    };
    for attempt in 0..JOIN_ATTEMPTS {
        let last = attempt + 1 == JOIN_ATTEMPTS;
        for (i, seed) in seeds.iter().enumerate() {
            let last_seed = last && i + 1 == seeds.len();
            match send_join_request(&**quic, seed.node_id, &request).await {
                // A restart of an already-joined node is a no-op, not a failure.
                Ok(
                    JoinResponse::Accepted { .. }
                    | JoinResponse::Rejected {
                        reason: JoinRejection::Duplicate,
                    },
                ) => return Ok(()),
                Ok(JoinResponse::Rejected { reason }) => {
                    return Err(StartError::Join(format!(
                        "cluster rejected join: {reason:?}"
                    )));
                }
                Ok(JoinResponse::Redirect { leader }) if last_seed => {
                    return Err(StartError::Join(format!(
                        "no leader available to accept the join (hint: {leader:?})"
                    )));
                }
                Err(e) if last_seed => {
                    return Err(StartError::Join(format!("join request failed: {e}")));
                }
                Ok(JoinResponse::Redirect { .. }) | Err(_) => {}
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    Err(StartError::Join(
        "join did not commit before the retry budget elapsed".to_string(),
    ))
}

/// Pre-join handshake: leader assigns a node id before this node starts Raft.
pub(crate) async fn join_cluster_auto(
    quic: &Arc<QuicTransport>,
    seeds: &[Seed],
    advertise: SocketAddr,
    role: JoinRole,
) -> Result<(NodeId, Membership), StartError> {
    debug_assert!(
        !seeds.is_empty(),
        "join_cluster_auto requires at least one seed"
    );

    let mut booked = false;
    let mut last_err = String::from("no seeds");
    'book: for attempt in 0..JOIN_ATTEMPTS {
        for seed in seeds {
            match fetch_peers(&**quic, seed.node_id).await {
                Ok(book) => {
                    for entry in book.entries {
                        if let Ok(addr) = entry.addr.parse::<SocketAddr>() {
                            quic.learn_peer(entry.node, addr);
                        }
                    }
                    booked = true;
                    break 'book;
                }
                Err(e) => last_err = e.to_string(),
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    if !booked {
        return Err(StartError::Join(format!(
            "no seed reachable to bootstrap discovery: {last_err}"
        )));
    }

    let request = JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        node_id: None,
        advertise_addr: advertise.to_string(),
        role,
    };
    for attempt in 0..JOIN_ATTEMPTS {
        let last = attempt + 1 == JOIN_ATTEMPTS;
        for (i, seed) in seeds.iter().enumerate() {
            let last_seed = last && i + 1 == seeds.len();
            match send_join_request(&**quic, seed.node_id, &request).await {
                Ok(JoinResponse::Accepted {
                    node_id,
                    membership,
                    ..
                }) => return Ok((node_id, membership)),
                Ok(JoinResponse::Rejected { reason }) => {
                    return Err(StartError::Join(format!(
                        "cluster rejected auto join: {reason:?}"
                    )));
                }
                Ok(JoinResponse::Redirect { leader }) if last_seed => {
                    return Err(StartError::Join(format!(
                        "no leader available to assign node id (hint: {leader:?})"
                    )));
                }
                Err(e) if last_seed => {
                    return Err(StartError::Join(format!("auto join request failed: {e}")));
                }
                Ok(JoinResponse::Redirect { .. }) | Err(_) => {}
            }
        }
        if attempt + 1 < JOIN_ATTEMPTS {
            tokio::time::sleep(JOIN_BACKOFF).await;
        }
    }
    Err(StartError::Join(
        "auto join did not commit before the retry budget elapsed".to_string(),
    ))
}
