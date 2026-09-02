//! Peer address book — maps each [`NodeId`] to the socket it listens on and
//! builds the HTTPS URLs used to reach it (backlog C1).
//!
//! This is deliberately transport-agnostic state: the QUIC connection pool
//! (C5) layers live connections on top of this directory, but the directory
//! itself is pure and cheap to clone, snapshot, and test.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use trembita_proto::NodeId;

use crate::route::Route;

/// A mapping from cluster [`NodeId`]s to their reachable socket addresses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerDirectory {
    peers: BTreeMap<NodeId, SocketAddr>,
}

impl PeerDirectory {
    /// An empty directory.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a peer's address, returning the previous address if the
    /// node was already known.
    pub fn insert(&mut self, id: NodeId, addr: SocketAddr) -> Option<SocketAddr> {
        self.peers.insert(id, addr)
    }

    /// Remove a peer (e.g. after it leaves the cluster), returning its address.
    pub fn remove(&mut self, id: NodeId) -> Option<SocketAddr> {
        self.peers.remove(&id)
    }

    /// The address for `id`, if known.
    #[must_use]
    pub fn addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.peers.get(&id).copied()
    }

    /// Whether `id` is present.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.peers.contains_key(&id)
    }

    /// Number of known peers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether the directory is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// All known node ids, ascending.
    #[must_use]
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.peers.keys().copied().collect()
    }

    /// Iterate over `(NodeId, SocketAddr)` pairs in ascending id order.
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, SocketAddr)> + '_ {
        self.peers.iter().map(|(&id, &addr)| (id, addr))
    }

    /// Build the absolute HTTPS URL to reach `route` on peer `id`, e.g.
    /// `https://10.0.0.2:7443/raft/v1/peer/wire`. Returns `None` if the peer is
    /// unknown. IPv6 addresses are bracketed by [`SocketAddr`]'s own `Display`.
    #[must_use]
    pub fn url(&self, id: NodeId, route: Route) -> Option<String> {
        self.addr(id)
            .map(|addr| format!("https://{addr}{}", route.path()))
    }
}

impl FromIterator<(NodeId, SocketAddr)> for PeerDirectory {
    fn from_iter<T: IntoIterator<Item = (NodeId, SocketAddr)>>(iter: T) -> Self {
        Self {
            peers: iter.into_iter().collect(),
        }
    }
}
