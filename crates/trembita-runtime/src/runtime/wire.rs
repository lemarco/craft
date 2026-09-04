use trembita_proto::{ClientResponse, NodeId, RaftRpc};

/// Encode a state-machine response as a successful client response body.
pub(super) fn encode_client_ok<R: serde::Serialize>(response: &R) -> ClientResponse {
    match trembita_proto::encode(response) {
        Ok(bytes) => ClientResponse::Ok(bytes),
        Err(e) => ClientResponse::Error(format!("encode response: {e}")),
    }
}

/// The sending node id carried inside a peer RPC payload. Until per-connection
/// certificate identity is wired (backlog C5), the runtime trusts the id the
/// RPC declares — safe on an mTLS-authenticated cluster where every peer is
/// CA-issued.
pub(super) fn rpc_sender(rpc: &RaftRpc) -> NodeId {
    match rpc {
        RaftRpc::RequestVote(rv) => rv.candidate_id,
        RaftRpc::AppendEntries(ae) => ae.leader_id,
        RaftRpc::InstallSnapshot(is) => is.leader_id,
    }
}
