//! Environment parsing shared by [`CraftyApp`](super::app::CraftyApp) and reference binaries.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;

use crate::discovery::Seed;
use crate::security::Security;
use crate::{CertPaths, NodeId, PemSecurity, cert_paths_from_env};
use crafty_actor::DEFAULT_DRAIN_TIMEOUT;
use crafty_net::PeerDirectory;

/// Parsed product-app configuration from the environment.
pub struct AppConfig {
    /// This node's id.
    pub node_id: NodeId,
    /// QUIC listen address.
    pub listen: SocketAddr,
    /// Admin HTTP listen address (`None` when disabled).
    pub admin: Option<SocketAddr>,
    /// Optional admin TLS PEM paths.
    pub admin_tls: Option<(PathBuf, PathBuf)>,
    /// Peer address book.
    pub peers: PeerDirectory,
    /// Static cluster members.
    pub members: Vec<NodeId>,
    /// Dynamic join seeds.
    pub join_seeds: Vec<Seed>,
    /// Accept dynamic joins.
    pub allow_join: bool,
    /// Accept cluster leave RPC.
    pub allow_leave: bool,
    /// Graceful leave on shutdown.
    pub graceful_leave: bool,
    /// Loaded mTLS identity.
    pub security: Security,
    /// On-disk PEM paths when configured.
    pub pem_paths: Option<CertPaths>,
    /// Actor drain timeout.
    pub drain_timeout: Duration,
    /// Persistent data directory.
    pub data_dir: Option<PathBuf>,
    /// Optional job queue stream (requires `data_dir`).
    pub job_queue_stream: Option<String>,
    /// Job queue lease timeout.
    pub job_queue_lease: Duration,
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

/// Parse `CRAFTY_NODE_ID` (default `1`).
pub fn node_id_from_env() -> Result<NodeId, Box<dyn Error>> {
    if let Some(raw) = env("CRAFTY_NODE_ID") {
        return Ok(NodeId(raw.parse()?));
    }
    Ok(NodeId(1))
}

/// Resolve `host:port` with brief DNS retry.
pub fn resolve_addr(hostport: &str) -> Result<SocketAddr, Box<dyn Error>> {
    if let Ok(addr) = hostport.parse::<SocketAddr>() {
        return Ok(addr);
    }
    let mut last = String::new();
    for _ in 0..20 {
        match hostport.to_socket_addrs() {
            Ok(mut addrs) => {
                if let Some(addr) = addrs.next() {
                    return Ok(addr);
                }
                last = format!("no addresses for {hostport:?}");
            }
            Err(e) => last = format!("cannot resolve {hostport:?}: {e}"),
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(last.into())
}

/// Parse `CRAFTY_PEERS` (`id@host:port,...`).
pub fn parse_peers(raw: &str) -> Result<(PeerDirectory, Vec<NodeId>), Box<dyn Error>> {
    let mut map = BTreeMap::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad CRAFTY_PEERS entry {entry:?} (want id@host:port)"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        map.insert(NodeId(id), resolve_addr(addr)?);
    }
    let members = map.keys().copied().collect();
    Ok((map.into_iter().collect(), members))
}

/// Parse `CRAFTY_JOIN_SEEDS`.
pub fn parse_seeds(raw: &str) -> Result<Vec<Seed>, Box<dyn Error>> {
    let mut seeds = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad CRAFTY_JOIN_SEEDS entry {entry:?}"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        seeds.push(Seed::new(NodeId(id), resolve_addr(addr)?));
    }
    Ok(seeds)
}

fn load_security_from_env(
    node_id: NodeId,
    members: &[NodeId],
    joining: bool,
) -> Result<(Security, Option<CertPaths>), Box<dyn Error>> {
    match (
        env("CRAFTY_NODE_CERT"),
        env("CRAFTY_NODE_KEY"),
        env("CRAFTY_CA_CERT"),
    ) {
        (Some(cert), Some(key), Some(ca)) => {
            let paths = cert_paths_from_env(&cert, &key, &ca);
            let loaded = PemSecurity::load(node_id, paths.clone())?;
            Ok((loaded.security, Some(paths)))
        }
        (None, None, None) => {
            if members.len() > 1 || joining {
                return Err("multi-node clusters need CRAFTY_NODE_CERT/KEY/CA_CERT".into());
            }
            #[cfg(feature = "dev-certs")]
            {
                let ca = crafty_net::tls::ClusterCa::generate()?;
                Ok((Security::dev(&ca, node_id)?, None))
            }
            #[cfg(not(feature = "dev-certs"))]
            {
                Err(
                    "enable crafty `dev-certs` feature or provide CRAFTY_NODE_CERT/KEY/CA_CERT"
                        .into(),
                )
            }
        }
        _ => Err(
            "set all of CRAFTY_NODE_CERT, CRAFTY_NODE_KEY, CRAFTY_CA_CERT together, or none".into(),
        ),
    }
}

fn drain_timeout_from_env() -> Duration {
    env("CRAFTY_DRAIN_TIMEOUT")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(DEFAULT_DRAIN_TIMEOUT, Duration::from_secs)
}

/// Load [`AppConfig`] from standard `CRAFTY_*` environment variables.
///
/// # Errors
/// Returns an error when required variables are missing or invalid.
pub fn app_config_from_env() -> Result<AppConfig, Box<dyn Error>> {
    let node_id = node_id_from_env()?;
    let listen: SocketAddr = env("CRAFTY_LISTEN")
        .as_deref()
        .unwrap_or("0.0.0.0:7443")
        .parse()?;
    let admin = match env("CRAFTY_ADMIN").as_deref() {
        Some("-") => None,
        Some(a) => Some(a.parse()?),
        None => Some("0.0.0.0:8080".parse()?),
    };
    let join_seeds = match env("CRAFTY_JOIN_SEEDS") {
        Some(raw) => parse_seeds(&raw)?,
        None => Vec::new(),
    };
    let joining = !join_seeds.is_empty();
    let (mut peers, mut members) = match env("CRAFTY_PEERS") {
        Some(raw) => parse_peers(&raw)?,
        None => (PeerDirectory::new(), Vec::new()),
    };
    if !joining {
        if !members.contains(&node_id) {
            members.push(node_id);
            members.sort();
        }
        if !peers.contains(node_id) {
            peers.insert(node_id, listen);
        }
    }
    let (security, pem_paths) = load_security_from_env(node_id, &members, joining)?;
    let data_dir = env("CRAFTY_DATA_DIR").map(PathBuf::from);
    let job_queue_stream = env("CRAFTY_JOB_QUEUE");
    if job_queue_stream.is_some() && data_dir.is_none() {
        return Err("CRAFTY_JOB_QUEUE requires CRAFTY_DATA_DIR".into());
    }
    let job_queue_lease = env("CRAFTY_JOB_QUEUE_LEASE_SECS")
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs);
    let admin_tls = match (env("CRAFTY_ADMIN_TLS_CERT"), env("CRAFTY_ADMIN_TLS_KEY")) {
        (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
        (None, None) => None,
        _ => {
            return Err(
                "CRAFTY_ADMIN_TLS_CERT and CRAFTY_ADMIN_TLS_KEY must both be set or both unset"
                    .into(),
            );
        }
    };

    Ok(AppConfig {
        node_id,
        listen,
        admin,
        admin_tls,
        peers,
        members,
        join_seeds,
        allow_join: env_bool("CRAFTY_ALLOW_JOIN"),
        allow_leave: env_bool("CRAFTY_ALLOW_LEAVE"),
        graceful_leave: env_bool("CRAFTY_GRACEFUL_LEAVE"),
        security,
        pem_paths,
        drain_timeout: drain_timeout_from_env(),
        data_dir,
        job_queue_stream,
        job_queue_lease,
    })
}
