use trembita_proto::{ClientResponse, ClientWireError, NodeId, RaftRpc};

use super::types::ClientError;

/// Encode a state-machine response as a successful client response body.
pub(super) fn encode_client_ok<R: serde::Serialize>(response: &R) -> ClientResponse {
    match trembita_proto::encode(response) {
        Ok(bytes) => ClientResponse::Ok(bytes),
        Err(e) => ClientResponse::Err(ClientWireError::EncodeResponse(e.to_string())),
    }
}

/// Map a runtime [`ClientError`] onto the wire error enum.
pub(super) fn runtime_error_to_wire(err: ClientError) -> ClientWireError {
    match err {
        ClientError::Stopped => ClientWireError::Stopped,
        ClientError::Driver(msg) => ClientWireError::Driver(msg),
        ClientError::NotLeader { leader } => {
            ClientWireError::Driver(format!("not leader (leader hint: {leader:?})"))
        }
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
