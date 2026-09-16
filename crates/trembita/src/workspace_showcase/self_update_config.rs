//! Env parsing for the self-update workspace showcase.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use crate::NodeId;
use crate::cluster::{CertPaths, PemSecurity, Security, cert_paths_from_env};
use crate::discovery::Seed;
use crate::net::PeerDirectory;
use crate::net::tls::ClusterCa;

const DATA_DIR_NAME: &str = "trembita-showcase-self-update";

/// Parsed node boot configuration.
pub struct NodeConfig {
    pub node_id: NodeId,
    pub listen: SocketAddr,
    pub gateway: Option<SocketAddr>,
    pub members: Vec<NodeId>,
    pub join_seeds: Vec<Seed>,
    pub allow_join: bool,
    pub allow_leave: bool,
    pub graceful_leave: bool,
    pub data_dir: Option<PathBuf>,
    pub security: Security,
    pub peers: PeerDirectory,
    pub pem_paths: Option<CertPaths>,
}

pub fn init_tracing() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("TREMBITA_LOG"))
        .unwrap_or_else(|_| "info,trembita=info".into());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

pub fn startup() {
    eprintln!(
        "self-update showcase — upgrade-coordinator demo (TREMBITA_UPGRADE_DRY_RUN=1 skips exit)"
    );
}

pub fn ready(cluster: &crate::cluster::TrembitaCluster<crate::UpgradeMachine>) {
    eprintln!(
        "node {:?} ready — members {:?}",
        cluster.node_id(),
        cluster.members()
    );
}

pub fn shutdown() {
    eprintln!("shutting down…");
}

fn default_data_dir() -> PathBuf {
    std::env::var("TREMBITA_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join(DATA_DIR_NAME))
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_bool(key: &str) -> bool {
    matches!(
        env(key).as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
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
                return Err(
                    "multi-node cluster requires TREMBITA_* cert env (see docs/certs.md)".into(),
                );
            }
            let ca = ClusterCa::generate()?;
            Ok((Security::dev(&ca, node_id)?, None))
        }
        _ => Err(
            "set all TREMBITA_NODE_CERT, TREMBITA_NODE_KEY, TREMBITA_CA_CERT or none for dev"
                .into(),
        ),
    }
}

pub fn config_from_env() -> Result<NodeConfig, Box<dyn Error>> {
    let node_id = NodeId(
        env("TREMBITA_NODE_ID")
            .unwrap_or_else(|| "1".into())
            .parse()?,
    );
    let listen: SocketAddr = env("TREMBITA_LISTEN")
        .unwrap_or_else(|| "0.0.0.0:7443".into())
        .parse()?;
    let gateway = env("TREMBITA_GATEWAY").map(|s| s.parse()).transpose()?;
    let join_seeds = env("TREMBITA_JOIN_SEEDS")
        .map(|s| parse_seeds(&s))
        .transpose()?
        .unwrap_or_default();
    let (peers, members) = env("TREMBITA_PEERS")
        .map(|s| parse_peers(&s))
        .transpose()?
        .unwrap_or_else(|| (PeerDirectory::new(), vec![node_id]));
    let data_dir = Some(default_data_dir());
    let (security, pem_paths) = load_security(node_id, members.as_slice(), !join_seeds.is_empty())?;
    Ok(NodeConfig {
        node_id,
        listen,
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
