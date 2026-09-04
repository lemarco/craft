use std::time::Duration;

use trembita_net::RemoteError;
use trembita_proto::{CatalogRejection, LeaveRejection, NodeId};
use trembita_runtime::ClusterScaleError;

/// How long [`super::TrembitaCluster::scale_cluster`] keeps re-resolving
/// the leader while a forwarded scale is transiently refused because leadership
/// is still settling. Comfortably exceeds the facts-refresh period so a scale
/// issued right after an election succeeds rather than failing spuriously.
pub(super) const SCALE_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
/// Delay between forward retries within [`SCALE_FORWARD_TIMEOUT`].
pub(super) const SCALE_FORWARD_RETRY: Duration = Duration::from_millis(25);
pub(super) const CATALOG_ADD_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const CATALOG_ADD_RETRY: Duration = Duration::from_millis(25);
/// Total budget for [`super::TrembitaCluster::leave`] peer retries.
pub(super) const LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between leave attempts within [`LEAVE_TIMEOUT`].
pub(super) const LEAVE_RETRY: Duration = Duration::from_millis(50);

/// Why [`super::TrembitaCluster::leave`] failed.
#[derive(Debug, thiserror::Error)]
pub enum LeaveError {
    /// No other member is configured to contact.
    #[error("no peer to submit leave to")]
    NoContact,
    /// Retries against live peers were exhausted.
    #[error("leave did not commit before deadline")]
    Timeout,
    /// The leader refused the leave request.
    #[error("leave rejected: {0:?}")]
    Rejected(LeaveRejection),
    /// A peer was unreachable or the wire framing failed.
    #[error(transparent)]
    Transport(#[from] trembita_net::TransportError),
}

/// Why a cluster-wide [`scale_cluster`](super::TrembitaCluster::scale_cluster) failed.
#[derive(Debug, thiserror::Error)]
pub enum ScaleClusterError {
    /// The node runtime has stopped, so its consensus status is unavailable.
    #[error("node runtime has stopped")]
    Stopped,
    /// No leader is currently elected to accept the scale.
    #[error("no leader is currently elected")]
    NoLeader,
    /// The actor config could not be encoded for forwarding.
    #[error("config encode failed: {0}")]
    Config(String),
    /// Planning or executing the scale on the leader failed.
    #[error(transparent)]
    Scale(#[from] ClusterScaleError),
    /// Forwarding the request to the leader failed (shipping to the leader, or
    /// the leader rejecting the scale).
    #[error(transparent)]
    Remote(#[from] RemoteError),
}

/// Why [`super::TrembitaCluster::add_raft_groups`] failed.
#[derive(Debug, thiserror::Error)]
pub enum AddRaftGroupsError {
    /// Multi-Raft catalog expansion is not enabled on this cluster.
    #[error("multi-raft catalog expansion is not enabled")]
    NotMultiRaft,
    /// The requested group count must be at least 1.
    #[error("add_groups must be at least 1")]
    InvalidCount,
    /// The node runtime has stopped, so catalog updates are unavailable.
    #[error("node runtime has stopped")]
    Stopped,
    /// No leader is currently elected to accept the catalog add.
    #[error("no leader is currently elected")]
    NoLeader,
    /// The catalog leader rejected the add request.
    #[error("catalog add rejected: {0:?}")]
    Rejected(CatalogRejection),
    /// A peer was unreachable or the wire framing failed.
    #[error(transparent)]
    Transport(#[from] trembita_net::TransportError),
}

impl From<CatalogAddLocalError> for AddRaftGroupsError {
    fn from(err: CatalogAddLocalError) -> Self {
        match err {
            CatalogAddLocalError::Stopped | CatalogAddLocalError::NoMetaRaftHandle => Self::Stopped,
            CatalogAddLocalError::NotLeader { .. } => Self::NoLeader,
            CatalogAddLocalError::Rejected(reason) => Self::Rejected(reason),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CatalogAddLocalError {
    #[error("node runtime has stopped")]
    Stopped,
    #[error("meta raft group is not hosted on this node")]
    NoMetaRaftHandle,
    #[error("not leader")]
    NotLeader { leader: Option<NodeId> },
    #[error("catalog add rejected: {0:?}")]
    Rejected(CatalogRejection),
}
