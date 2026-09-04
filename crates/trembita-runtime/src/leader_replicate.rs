//! Shared leader→voter replication helpers for product wire services.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::task::JoinSet;
use trembita_proto::NodeId;

use crate::supervisor::ClusterState;

/// Error when other voters exist in membership but none are reachable for replication.
pub const REPLICATION_NO_REACHABLE_VOTERS: &str =
    "replication failed: other voters exist but none are reachable";

type ReplicateSendFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// Peers to fan out replication RPCs to (reachable voters except `node_id`).
///
/// Returns an empty vec when this node is the sole voter (single-node cluster).
/// Returns an error when other voters exist but none are reachable.
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
pub fn authorize_replicate_leader(
    state: &dyn ClusterState,
    declared_leader: NodeId,
    not_leader_msg: &str,
) -> Result<(), String> {
    let Some(leader) = state.leader_id() else {
        return Err("no raft leader elected".to_string());
    };
    if declared_leader != leader {
        return Err(not_leader_msg.to_string());
    }
    Ok(())
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
