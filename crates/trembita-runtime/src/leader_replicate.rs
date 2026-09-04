//! Shared leader→voter replication helpers for product wire services.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::task::JoinSet;
use trembita_net::transport::{BoxFuture, TransportError};
use trembita_proto::{NodeId, ProductWireError};

use crate::supervisor::ClusterState;

/// Error when other voters exist in membership but none are reachable for replication.
pub const REPLICATION_NO_REACHABLE_VOTERS: &str =
    "replication failed: other voters exist but none are reachable";

type ReplicateSendFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// Peers to fan out replication RPCs to (reachable voters except `node_id`).
///
/// Returns an empty vec when this node is the sole voter (single-node cluster).
/// Returns an error when other voters exist but none are reachable.
///
/// # Errors
/// When other voters are configured but none are currently reachable.
pub fn replication_peers(state: &dyn ClusterState, node_id: NodeId) -> Result<Vec<NodeId>, String> {
    let other_voters: Vec<NodeId> = state
        .live_nodes()
        .into_iter()
        .filter(|id| *id != node_id)
        .collect();
    if other_voters.is_empty() {
        return Ok(Vec::new());
    }
    let peers: Vec<NodeId> = state
        .reachable_nodes()
        .into_iter()
        .filter(|id| *id != node_id)
        .collect();
    if peers.is_empty() {
        return Err(REPLICATION_NO_REACHABLE_VOTERS.to_string());
    }
    Ok(peers)
}

/// Run `send` against every peer in parallel; all must succeed.
///
/// # Errors
/// Propagates the first peer failure or join error.
pub async fn fanout_replicate(
    peers: &[NodeId],
    send: impl Fn(NodeId) -> ReplicateSendFuture + Send + Sync + 'static,
) -> Result<(), String> {
    if peers.is_empty() {
        return Ok(());
    }
    let send = Arc::new(send);
    let mut set = JoinSet::new();
    for &peer in peers {
        let send = Arc::clone(&send);
        set.spawn(async move { send(peer).await });
    }
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

/// Verify `declared_leader` matches the current Raft leader hint.
///
/// # Errors
/// When no leader is elected or `declared_leader` does not match.
pub fn authorize_replicate_leader(
    state: &dyn ClusterState,
    declared_leader: NodeId,
    not_leader_msg: &str,
) -> Result<(), ProductWireError> {
    let Some(leader) = state.leader_id() else {
        return Err(ProductWireError::NoLeaderElected);
    };
    if declared_leader != leader {
        let _ = not_leader_msg;
        return Err(ProductWireError::ReplicateNotLeader);
    }
    Ok(())
}

/// Forward an RPC to the current Raft leader.
///
/// # Errors
/// When no leader is elected or the transport call fails.
pub async fn forward_to_leader<R>(
    state: &dyn ClusterState,
    send: impl FnOnce(NodeId) -> BoxFuture<'static, Result<R, TransportError>>,
) -> Result<R, ProductWireError> {
    let leader = state.leader_id().ok_or(ProductWireError::NoLeaderElected)?;
    send(leader)
        .await
        .map_err(|e| ProductWireError::ForwardFailed {
            leader,
            reason: e.to_string(),
        })
}

/// Resolve replication peers and fan out; maps string errors to [`ProductWireError`].
///
/// # Errors
/// When peer resolution or any fan-out RPC fails.
pub async fn fanout_product_replicate(
    state: &dyn ClusterState,
    node_id: NodeId,
    send: impl Fn(NodeId) -> ReplicateSendFuture + Send + Sync + 'static,
) -> Result<(), ProductWireError> {
    let peers = replication_peers(state, node_id).map_err(ProductWireError::classify)?;
    fanout_replicate(&peers, send)
        .await
        .map_err(ProductWireError::classify)
}

/// Extract optional wire error from a replicate reply and map to fanout failure.
///
/// # Errors
/// When the replicate reply carries an error payload.
pub fn replicate_reply_err(error: Option<ProductWireError>) -> Result<(), ProductWireError> {
    match error {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::ClusterState;

    struct MockState {
        live: Vec<NodeId>,
        reachable: Vec<NodeId>,
    }

    impl ClusterState for MockState {
        fn is_leader(&self) -> bool {
            true
        }

        fn live_nodes(&self) -> Vec<NodeId> {
            self.live.clone()
        }

        fn reachable_nodes(&self) -> Vec<NodeId> {
            self.reachable.clone()
        }
    }

    #[test]
    fn sole_voter_has_no_peers() {
        let state = MockState {
            live: vec![NodeId(1)],
            reachable: vec![NodeId(1)],
        };
        assert!(replication_peers(&state, NodeId(1)).unwrap().is_empty());
    }

    #[test]
    fn unreachable_other_voters_error() {
        let state = MockState {
            live: vec![NodeId(1), NodeId(2), NodeId(3)],
            reachable: vec![NodeId(1)],
        };
        let err = replication_peers(&state, NodeId(1)).unwrap_err();
        assert!(err.contains("reachable"));
    }

    #[test]
    fn partial_reachability_returns_subset() {
        let state = MockState {
            live: vec![NodeId(1), NodeId(2), NodeId(3)],
            reachable: vec![NodeId(1), NodeId(2)],
        };
        assert_eq!(
            replication_peers(&state, NodeId(1)).unwrap(),
            vec![NodeId(2)]
        );
    }
}
