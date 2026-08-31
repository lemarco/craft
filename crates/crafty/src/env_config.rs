//! Environment parsing shared by [`CraftyApp`](super::app::CraftyApp) and reference binaries.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;

use crate::NodeId;
use crate::certs::{PemSecurity, cert_paths_for_node, cert_paths_from_env};
use crate::discovery::Seed;
use crate::node_id;
use crate::security::Security;
use crafty_actor::DEFAULT_DRAIN_TIMEOUT;
use crafty_net::CertPaths;
use crafty_net::PeerDirectory;

/// Parsed product-app configuration from the environment.
#[allow(clippy::struct_excessive_bools)] // env toggles map 1:1 to optional features.
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
    /// Shared cert directory; per-node PEMs are picked after id resolution.
    pub cert_dir: Option<PathBuf>,
    /// Actor drain timeout.
    pub drain_timeout: Duration,
    /// Persistent data directory.
    pub data_dir: Option<PathBuf>,
    /// Optional job queue stream (requires `data_dir`).
    pub job_queue_stream: Option<String>,
    /// Job queue lease timeout.
    pub job_queue_lease: Duration,
    /// Optional product gateway listen address (`None` when disabled).
    pub gateway: Option<SocketAddr>,
    /// Mount tier C `/jobs/*` on the gateway when `gateway` is set (`CRAFTY_GATEWAY_JOBS=1`).
    pub gateway_jobs_api: bool,
    /// Mount `/actors/*` cast + ask on the gateway when `gateway` is set (`CRAFTY_GATEWAY_ACTORS=1`).
    pub gateway_actors_api: bool,
    /// Mount `/workflows/*` on the gateway when `gateway` is set (`CRAFTY_GATEWAY_WORKFLOWS=1`).
    pub gateway_workflows_api: bool,
    /// Product gateway connection drain timeout (`CRAFTY_GATEWAY_DRAIN_TIMEOUT`).
    pub gateway_drain_timeout: Duration,
    /// Optional gateway TLS PEM paths (`CRAFTY_GATEWAY_TLS_*`).
    pub gateway_tls: Option<(PathBuf, PathBuf)>,
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

/// Process role — **advanced production split only**.
///
/// Product showcases run the same binary on every node (gateway + workers). Use
/// [`NodeRole::Gateway`] only when you deliberately want edge-only ingress without
/// local consumers or supervised actors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Gateway and workers/consumers on the same node (local dev default).
    Both,
    /// Edge node: HTTP/WebSocket ingress without local workers or consumers.
    Gateway,
    /// Worker-only node (still honors `CRAFTY_GATEWAY` when set).
    Worker,
}

/// Parse [`NodeRole`] from `CRAFTY_ROLE` with legacy env fallbacks.
///
/// Legacy mapping (when `CRAFTY_ROLE` is unset):
/// - `CRAFTY_NO_CONSUMER=1`, `CRAFTY_GATEWAY_ONLY=1`, or `GATEWAY=1` → [`NodeRole::Gateway`]
#[must_use]
pub fn node_role_from_env() -> NodeRole {
    if let Some(raw) = env("CRAFTY_ROLE") {
        return match raw.to_ascii_lowercase().as_str() {
            "gateway" | "edge" => NodeRole::Gateway,
            "worker" => NodeRole::Worker,
            _ => NodeRole::Both,
        };
    }
    if env_bool("CRAFTY_NO_CONSUMER")
        || env_bool("CRAFTY_GATEWAY_ONLY")
        || env("GATEWAY").as_deref() == Some("1")
    {
        return NodeRole::Gateway;
    }
    NodeRole::Both
}

/// Whether this node should skip tier C consumers (`#[consumer]` loops).
///
/// **Advanced:** returns `false` when `CRAFTY_ROLE=gateway`. Showcases do not set this.
#[must_use]
pub fn consumers_enabled_from_env() -> bool {
    !matches!(node_role_from_env(), NodeRole::Gateway)
}

/// Whether this node should register supervised actors / auto-scaled workers.
///
/// **Advanced:** returns `false` when `CRAFTY_ROLE=gateway`. Showcases do not set this.
#[must_use]
pub fn workers_enabled_from_env() -> bool {
    !matches!(node_role_from_env(), NodeRole::Gateway)
}

/// Whether this node is configured as gateway-only (no local workers/consumers).
#[must_use]
pub fn gateway_only_from_env() -> bool {
    matches!(node_role_from_env(), NodeRole::Gateway)
}

/// Parse `CRAFTY_NODE_ID` when explicitly set (otherwise id comes from disk or assignment).
pub fn node_id_from_env() -> Option<NodeId> {
    env("CRAFTY_NODE_ID")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(NodeId)
}

/// Resolve the node id before boot: persisted file, explicit env, or seed default.
fn resolve_node_id(data_dir: Option<&PathBuf>, joining: bool) -> NodeId {
    if let Some(dir) = data_dir
        && let Some(id) = node_id::read_persisted(dir)
    {
        return id;
    }
    if let Some(id) = node_id_from_env() {
        return id;
    }
    if joining { NodeId(0) } else { NodeId(1) }
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
    cert_dir: Option<&std::path::Path>,
) -> Result<(Security, Option<CertPaths>), Box<dyn Error>> {
    if let Some(dir) = cert_dir {
        let tls_id = if node_id == NodeId(0) {
            NodeId(1)
        } else {
            node_id
        };
        let paths = cert_paths_for_node(dir, tls_id);
        let loaded = PemSecurity::load(tls_id, paths.clone())?;
        return Ok((loaded.security, Some(paths)));
    }
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
                return Err(
                    "multi-node clusters need CRAFTY_CERT_DIR or CRAFTY_NODE_CERT/KEY/CA_CERT"
                        .into(),
                );
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

fn gateway_drain_timeout_from_env() -> Duration {
    env("CRAFTY_GATEWAY_DRAIN_TIMEOUT")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(
            crate::gateway::DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            Duration::from_secs,
        )
}

/// Load [`AppConfig`] from standard `CRAFTY_*` environment variables.
///
/// # Errors
/// Returns an error when required variables are missing or invalid.
#[allow(clippy::too_many_lines)]
pub fn app_config_from_env() -> Result<AppConfig, Box<dyn Error>> {
    let listen: SocketAddr = env("CRAFTY_LISTEN")
        .as_deref()
        .unwrap_or("0.0.0.0:7443")
        .parse()?;
    let data_dir = env("CRAFTY_DATA_DIR").map(PathBuf::from);
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
    let node_id = resolve_node_id(data_dir.as_ref(), joining);
    let allow_join = match env("CRAFTY_ALLOW_JOIN") {
        Some(_) => env_bool("CRAFTY_ALLOW_JOIN"),
        None => !joining,
    };
    let allow_leave = match env("CRAFTY_ALLOW_LEAVE") {
        Some(_) => env_bool("CRAFTY_ALLOW_LEAVE"),
        None => true,
    };
    let graceful_leave = match env("CRAFTY_GRACEFUL_LEAVE") {
        Some(_) => env_bool("CRAFTY_GRACEFUL_LEAVE"),
        None => true,
    };
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
    let cert_dir = env("CRAFTY_CERT_DIR").map(PathBuf::from);
    let (security, pem_paths) =
        load_security_from_env(node_id, &members, joining, cert_dir.as_deref())?;
    let job_queue_stream = env("CRAFTY_JOB_QUEUE");
    if job_queue_stream.is_some() && data_dir.is_none() {
        return Err("CRAFTY_JOB_QUEUE requires CRAFTY_DATA_DIR".into());
    }
    let job_queue_lease = env("CRAFTY_JOB_QUEUE_LEASE_SECS")
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs);
    let gateway = match env("CRAFTY_GATEWAY").as_deref() {
        Some("-") | None => None,
        Some(a) => Some(a.parse()?),
    };
    let gateway_jobs_api = env_bool("CRAFTY_GATEWAY_JOBS");
    let gateway_actors_api = env_bool("CRAFTY_GATEWAY_ACTORS");
    let gateway_workflows_api = env_bool("CRAFTY_GATEWAY_WORKFLOWS");
    let gateway_tls = match (
        env("CRAFTY_GATEWAY_TLS_CERT"),
        env("CRAFTY_GATEWAY_TLS_KEY"),
    ) {
        (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
        (None, None) => None,
        _ => {
            return Err(
                "CRAFTY_GATEWAY_TLS_CERT and CRAFTY_GATEWAY_TLS_KEY must both be set or both unset"
                    .into(),
            );
        }
    };
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
        allow_join,
        allow_leave,
        graceful_leave,
        security,
        pem_paths,
        cert_dir,
        drain_timeout: drain_timeout_from_env(),
        data_dir,
        job_queue_stream,
        job_queue_lease,
        gateway,
        gateway_jobs_api,
        gateway_actors_api,
        gateway_workflows_api,
        gateway_drain_timeout: gateway_drain_timeout_from_env(),
        gateway_tls,
    })
}
