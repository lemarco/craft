//! Persistent cluster node identity ([`NodeId`]) under `data_dir`.

use std::io;
use std::path::Path;

use crate::NodeId;

/// Filename written under [`TREMBITA_DATA_DIR`](super::env_config::AppConfig::data_dir).
pub const NODE_ID_FILE: &str = "node-id";

/// Read a previously assigned node id from `{data_dir}/node-id`.
#[must_use]
pub fn read_persisted(data_dir: &Path) -> Option<NodeId> {
    let raw = std::fs::read_to_string(data_dir.join(NODE_ID_FILE)).ok()?;
    let id: u64 = raw.trim().parse().ok()?;
    Some(NodeId(id))
}

/// Persist `node_id` to `{data_dir}/node-id` (created on first boot / join).
///
/// # Errors
/// Returns an I/O error when the directory or file cannot be written.
pub fn persist(data_dir: &Path, node_id: NodeId) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(data_dir.join(NODE_ID_FILE), format!("{}\n", node_id.0))
}
