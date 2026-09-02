//! Env + logging for the self-update showcase.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use trembita::cluster::{
    CertPaths, PemSecurity, Security, cert_paths_from_env,
};
use trembita::discovery::Seed;
use trembita::net::PeerDirectory;
use trembita::NodeId;
use trembita_tools::showcase_common::data_dir;

const DATA_DIR_NAME: &str = "trembita-showcase-self-update";

/// Parsed node boot configuration.
pub struct NodeConfig {
    /// This node's id.
    pub node_id: NodeId,
    /// QUIC listen address.
    pub listen: SocketAddr,
    /// Optional admin HTTP.
    pub admin: Option<SocketAddr>,
    /// Upgrade HTTP API.
    pub gateway: Option<SocketAddr>,
    /// Static membership list.
    pub members: Vec<NodeId>,
    /// Dynamic join seeds.
    pub join_seeds: Vec<Seed>,
    /// Accept dynamic joins.
    pub allow_join: bool,
    /// Accept cluster leave RPC.
    pub allow_leave: bool,
    /// Call `leave()` on shutdown.
    pub graceful_leave: bool,
    /// Persistent data directory.
    pub data_dir: Option<PathBuf>,
    /// mTLS security material.
    pub security: Security,
    /// Peer address book.
    pub peers: PeerDirectory,
    /// On-disk PEM paths when using [`PemSecurity`].
    pub pem_paths: Option<CertPaths>,
}

pub fn init_tracing() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("TREMBITA_LOG"))
        .unwrap_or_else(|_| "info,trembita=info".into());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init();
}

pub fn startup() {
    eprintln!(
        "self-update showcase — upgrade-coordinator demo (TREMBITA_UPGRADE_DRY_RUN=1 skips exit)"
    );
}

pub fn ready(cluster: &trembita::cluster::TrembitaCluster<trembita::UpgradeMachine>) {
    eprintln!(
        "node {:?} ready — members {:?}",
        cluster.node_id(),
        cluster.members()
    );
}

pub fn shutdown() {
    eprintln!("shutting down…");
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_bool(key: &str) -> bool {
    matches!(env(key).as_deref(), Some("1" | "true" | "TRUE" | "yes" | "on"))
}

fn resolve_addr(hostport: &str) -> Result<SocketAddr, Box<dyn Error>> {
    if let Ok(addr) = hostport.parse::<SocketAddr>() {
        return Ok(addr);
    }
    hostport
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("no addresses for {hostport}").into())
}

fn parse_peers(raw: &str) -> Result<(PeerDirectory, Vec<NodeId>), Box<dyn Error>> {
    let mut map = BTreeMap::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad TREMBITA_PEERS entry {entry:?}"))?;
        map.insert(NodeId(id.parse()?), resolve_addr(addr)?);
    }
    let members: Vec<NodeId> = map.keys().copied().collect();
    Ok((map.into_iter().collect(), members))
}

fn parse_seeds(raw: &str) -> Result<Vec<Seed>, Box<dyn Error>> {
    raw.split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|entry| {
            let (id, addr) = entry
                .split_once('@')
                .ok_or_else(|| format!("bad seed {entry:?}"))?;
            Ok(Seed::new(NodeId(id.parse()?), resolve_addr(addr)?))
        })
        .collect()
}

fn load_security(
    node_id: NodeId,
    members: &[NodeId],
    joining: bool,
) -> Result<(Security, Option<CertPaths>), Box<dyn Error>> {
    match (
        env("TREMBITA_NODE_CERT"),
        env("TREMBITA_NODE_KEY"),
        env("TREMBITA_CA_CERT"),
    ) {
        (Some(cert), Some(key), Some(ca)) => {
            let paths = cert_paths_from_env(cert, key, ca);
            let loaded = PemSecurity::load(node_id, paths.clone())?;
            Ok((loaded.security, Some(paths)))
        }
        (None, None, None) => {
            if members.len() > 1 || joining {
                return Err("multi-node cluster requires TREMBITA_* cert env (see docs/certs.md)".into());
            }
            let ca = trembita::net::tls::ClusterCa::generate()?;
            Ok((Security::dev(&ca, node_id)?, None))
        }
        _ => Err("set all TREMBITA_NODE_CERT, TREMBITA_NODE_KEY, TREMBITA_CA_CERT or none for dev".into()),
    }
}

/// Load configuration from standard `TREMBITA_*` environment variables.
pub fn config_from_env() -> Result<NodeConfig, Box<dyn Error>> {
    let node_id = NodeId(env("TREMBITA_NODE_ID").unwrap_or_else(|| "1".into()).parse()?);
    let listen: SocketAddr = env("TREMBITA_LISTEN")
        .unwrap_or_else(|| "0.0.0.0:7443".into())
        .parse()?;
    let admin = match env("TREMBITA_ADMIN").as_deref() {
        Some("-") => None,
        Some(s) => Some(s.parse()?),
        None => Some("127.0.0.1:8080".parse()?),
    };
    let gateway = env("TREMBITA_GATEWAY").map(|s| s.parse()).transpose()?;
    let join_seeds = env("TREMBITA_JOIN_SEEDS")
        .map(|s| parse_seeds(&s))
        .transpose()?
        .unwrap_or_default();
    let (peers, members) = env("TREMBITA_PEERS")
        .map(|s| parse_peers(&s))
        .transpose()?
        .unwrap_or_else(|| (PeerDirectory::new(), vec![node_id]));
    let data_dir = env("TREMBITA_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| Some(data_dir(DATA_DIR_NAME)));
    let (security, pem_paths) = load_security(node_id, members.as_slice(), !join_seeds.is_empty())?;
    Ok(NodeConfig {
        node_id,
        listen,
        admin,
        gateway,
        members: members.to_vec(),
        join_seeds,
        allow_join: env_bool("TREMBITA_ALLOW_JOIN"),
        allow_leave: env_bool("TREMBITA_ALLOW_LEAVE"),
        graceful_leave: env_bool("TREMBITA_GRACEFUL_LEAVE"),
        data_dir,
        security,
        peers,
        pem_paths,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peers_roundtrip() {
        let (peers, members) = parse_peers("1@127.0.0.1:7443,2@127.0.0.1:7444").unwrap();
        assert_eq!(members.len(), 2);
        assert!(peers.contains(NodeId(1)));
    }
}
