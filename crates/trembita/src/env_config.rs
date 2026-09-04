//! Environment parsing shared by [`TrembitaApp`](super::app::TrembitaApp) and reference binaries.

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
use trembita_net::CertPaths;
use trembita_net::PeerDirectory;
use trembita_proto::JoinRole;
use trembita_runtime::DEFAULT_DRAIN_TIMEOUT;

/// Which `TREMBITA_*` variables were explicitly set (vs derived defaults).
#[derive(Debug, Clone, Copy, Default)]
pub struct EnvOverrides {
    /// `TREMBITA_PEERS` was set.
    pub peers: bool,
    /// `TREMBITA_ALLOW_JOIN` was set.
    pub allow_join: bool,
    /// `TREMBITA_ALLOW_VOTER_JOIN` was set.
    pub allow_voter_join: bool,
    /// `TREMBITA_JOIN_ROLE` was set.
    pub join_role: bool,
    /// `TREMBITA_ALLOW_LEAVE` was set.
    pub allow_leave: bool,
    /// `TREMBITA_GRACEFUL_LEAVE` was set.
    pub graceful_leave: bool,
    /// `TREMBITA_DRAIN_TIMEOUT` was set.
    pub drain_timeout: bool,
    /// `TREMBITA_CERT_WATCH_SECS` was set.
    pub cert_watch: bool,
    /// `TREMBITA_ADMIN` was set (including `-` to disable).
    pub admin: bool,
    /// `TREMBITA_VOTER_REPLACEMENT` was set.
    pub voter_replacement: bool,
    /// `TREMBITA_VOTER_REPLACEMENT_GRACE_TICKS` was set.
    pub voter_replacement_grace_ticks: bool,
    /// `TREMBITA_GATEWAY_INTROSPECT` was set.
    pub gateway_introspect: bool,
}

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
    /// Accept `/cluster/join` with [`JoinRole::Voter`] on this node (seed-side).
    pub allow_voter_join: bool,
    /// Role requested when this node dynamically joins another cluster.
    pub join_role: JoinRole,
    /// Accept cluster leave RPC.
    pub allow_leave: bool,
    /// Graceful leave on shutdown.
    pub graceful_leave: bool,
    /// Leader replaces unreachable voters when true.
    pub voter_replacement: bool,
    /// Override voter replacement grace window in logical ticks.
    pub voter_replacement_grace_ticks: Option<u64>,
    /// Loaded mTLS identity.
    pub security: Security,
    /// On-disk PEM paths when configured.
    pub pem_paths: Option<CertPaths>,
    /// Shared cert directory; per-node PEMs are picked after id resolution.
    pub cert_dir: Option<PathBuf>,
    /// Actor drain timeout.
    pub drain_timeout: Duration,
    /// PEM hot-reload poll interval when paths are configured.
    pub cert_watch: Duration,
    /// Persistent data directory.
    pub data_dir: Option<PathBuf>,
    /// Optional job queue stream (requires `data_dir`).
    pub job_queue_stream: Option<String>,
    /// Job queue lease timeout.
    pub job_queue_lease: Duration,
    /// Optional product gateway listen address (`None` when disabled).
    pub gateway: Option<SocketAddr>,
    /// Mount job queue `/jobs/*` on the gateway when `gateway` is set (`TREMBITA_GATEWAY_JOBS=1`).
    pub gateway_jobs_api: bool,
    /// Mount `/actors/*` cast + ask on the gateway when `gateway` is set (`TREMBITA_GATEWAY_ACTORS=1`).
    pub gateway_actors_api: bool,
    /// Mount `/workflows/*` on the gateway when `gateway` is set (`TREMBITA_GATEWAY_WORKFLOWS=1`).
    pub gateway_workflows_api: bool,
    /// Mount `/introspect/*` on the gateway when `gateway` is set (`TREMBITA_GATEWAY_INTROSPECT=1`).
    pub gateway_introspect_api: bool,
    /// Product gateway connection drain timeout (`TREMBITA_GATEWAY_DRAIN_TIMEOUT`).
    pub gateway_drain_timeout: Duration,
    /// Optional gateway TLS PEM paths (`TREMBITA_GATEWAY_TLS_*`).
    pub gateway_tls: Option<(PathBuf, PathBuf)>,
    /// Explicit env vars that were set for this parse.
    pub env: EnvOverrides,
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

fn join_role_from_env() -> Result<JoinRole, Box<dyn Error>> {
    match env("TREMBITA_JOIN_ROLE").as_deref() {
        None | Some("learner") => Ok(JoinRole::Learner),
        Some("voter") => Ok(JoinRole::Voter),
        Some(other) => {
            Err(format!("TREMBITA_JOIN_ROLE must be voter or learner (got {other:?})").into())
        }
    }
}

/// Parse `TREMBITA_CERT_WATCH_SECS` (default 60) for PEM hot reload polling.
#[must_use]
pub fn cert_watch_period_from_env() -> Duration {
    env("TREMBITA_CERT_WATCH_SECS")
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs)
}

/// Parse `TREMBITA_NODE_ID` when explicitly set (otherwise id comes from disk or assignment).
pub fn node_id_from_env() -> Option<NodeId> {
    env("TREMBITA_NODE_ID")
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

/// Parse `TREMBITA_PEERS` (`id@host:port,...`).
pub fn parse_peers(raw: &str) -> Result<(PeerDirectory, Vec<NodeId>), Box<dyn Error>> {
    let mut map = BTreeMap::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad TREMBITA_PEERS entry {entry:?} (want id@host:port)"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        map.insert(NodeId(id), resolve_addr(addr)?);
    }
    let members = map.keys().copied().collect();
    Ok((map.into_iter().collect(), members))
}

/// Parse `TREMBITA_JOIN_SEEDS`.
pub fn parse_seeds(raw: &str) -> Result<Vec<Seed>, Box<dyn Error>> {
    let mut seeds = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad TREMBITA_JOIN_SEEDS entry {entry:?}"))?;
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
        let paths = cert_paths_for_node(dir, node_id);
        let pem = PemSecurity::load(node_id, paths.clone())?;
        return Ok((pem.security, Some(paths)));
    }
    match (
        env("TREMBITA_NODE_CERT"),
        env("TREMBITA_NODE_KEY"),
        env("TREMBITA_CA_CERT"),
    ) {
        (Some(cert), Some(key), Some(ca)) => {
            let paths = cert_paths_from_env(cert, key, ca);
            let pem = PemSecurity::load(node_id, paths.clone())?;
            Ok((pem.security, Some(paths)))
        }
        (None, None, None) => {
            if members.len() > 1 || joining {
                return Err(
                    "multi-node clusters need TREMBITA_CERT_DIR or TREMBITA_NODE_CERT/KEY/CA_CERT"
                        .into(),
                );
            }
            #[cfg(feature = "dev-certs")]
            {
                let ca = trembita_net::tls::ClusterCa::generate()?;
                Ok((Security::dev(&ca, node_id)?, None))
            }
            #[cfg(not(feature = "dev-certs"))]
            {
                let _ = (node_id, members, joining);
                Err(
                    "enable trembita `dev-certs` feature or provide TREMBITA_NODE_CERT/KEY/CA_CERT"
                        .into(),
                )
            }
        }
        _ => Err(
            "set all of TREMBITA_NODE_CERT, TREMBITA_NODE_KEY, TREMBITA_CA_CERT together, or none for dev mode"
                .into(),
        ),
    }
}

fn drain_timeout_from_env() -> Duration {
    env("TREMBITA_DRAIN_TIMEOUT")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(DEFAULT_DRAIN_TIMEOUT, Duration::from_secs)
}

fn gateway_drain_timeout_from_env() -> Duration {
    env("TREMBITA_GATEWAY_DRAIN_TIMEOUT")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(
            crate::gateway::DEFAULT_GATEWAY_DRAIN_TIMEOUT,
            Duration::from_secs,
        )
}

/// Load [`AppConfig`] from standard `TREMBITA_*` environment variables.
///
/// # Errors
/// Returns an error when required variables are missing or invalid.
#[allow(clippy::too_many_lines)]
pub fn app_config_from_env() -> Result<AppConfig, Box<dyn Error>> {
    let mut env_overrides = EnvOverrides::default();

    let listen: SocketAddr = env("TREMBITA_LISTEN")
        .as_deref()
        .unwrap_or("0.0.0.0:7443")
        .parse()?;
    let data_dir = env("TREMBITA_DATA_DIR").map(PathBuf::from);
    let admin = match env("TREMBITA_ADMIN").as_deref() {
        Some("-") => {
            env_overrides.admin = true;
            None
        }
        Some(a) => {
            env_overrides.admin = true;
            Some(a.parse()?)
        }
        None => None,
    };
    let join_seeds = match env("TREMBITA_JOIN_SEEDS") {
        Some(raw) => parse_seeds(&raw)?,
        None => Vec::new(),
    };
    let joining = !join_seeds.is_empty();
    let node_id = resolve_node_id(data_dir.as_ref(), joining);
    let allow_join = match env("TREMBITA_ALLOW_JOIN") {
        Some(_) => {
            env_overrides.allow_join = true;
            env_bool("TREMBITA_ALLOW_JOIN")
        }
        None => !joining,
    };
    let allow_voter_join = if env("TREMBITA_ALLOW_VOTER_JOIN").is_some() {
        env_overrides.allow_voter_join = true;
        env_bool("TREMBITA_ALLOW_VOTER_JOIN")
    } else {
        false
    };
    let join_role = if env("TREMBITA_JOIN_ROLE").is_some() {
        env_overrides.join_role = true;
        join_role_from_env()?
    } else {
        JoinRole::Learner
    };
    let allow_leave = match env("TREMBITA_ALLOW_LEAVE") {
        Some(_) => {
            env_overrides.allow_leave = true;
            env_bool("TREMBITA_ALLOW_LEAVE")
        }
        None => true,
    };
    let graceful_leave = match env("TREMBITA_GRACEFUL_LEAVE") {
        Some(_) => {
            env_overrides.graceful_leave = true;
            env_bool("TREMBITA_GRACEFUL_LEAVE")
        }
        None => true,
    };
    let voter_replacement = match env("TREMBITA_VOTER_REPLACEMENT") {
        Some(_) => {
            env_overrides.voter_replacement = true;
            env_bool("TREMBITA_VOTER_REPLACEMENT")
        }
        None => true,
    };
    let voter_replacement_grace_ticks = match env("TREMBITA_VOTER_REPLACEMENT_GRACE_TICKS") {
        Some(raw) => {
            env_overrides.voter_replacement_grace_ticks = true;
            Some(raw.parse::<u64>()?)
        }
        None => None,
    };
    let (mut peers, mut members) = match env("TREMBITA_PEERS") {
        Some(raw) => {
            env_overrides.peers = true;
            parse_peers(&raw)?
        }
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
    let cert_dir = env("TREMBITA_CERT_DIR").map(PathBuf::from);
    let (security, pem_paths) =
        load_security_from_env(node_id, &members, joining, cert_dir.as_deref())?;
    let job_queue_stream = env("TREMBITA_JOB_QUEUE");
    if job_queue_stream.is_some() && data_dir.is_none() {
        return Err("TREMBITA_JOB_QUEUE requires TREMBITA_DATA_DIR".into());
    }
    let job_queue_lease = env("TREMBITA_JOB_QUEUE_LEASE_SECS")
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs);
    let gateway = match env("TREMBITA_GATEWAY").as_deref() {
        Some("-") | None => None,
        Some(a) => Some(a.parse()?),
    };
    let gateway_jobs_api = env_bool("TREMBITA_GATEWAY_JOBS");
    let gateway_actors_api = env_bool("TREMBITA_GATEWAY_ACTORS");
    let gateway_workflows_api = env_bool("TREMBITA_GATEWAY_WORKFLOWS");
    let gateway_introspect_api = if env("TREMBITA_GATEWAY_INTROSPECT").is_some() {
        env_overrides.gateway_introspect = true;
        env_bool("TREMBITA_GATEWAY_INTROSPECT")
    } else {
        false
    };
    let gateway_tls = match (
        env("TREMBITA_GATEWAY_TLS_CERT"),
        env("TREMBITA_GATEWAY_TLS_KEY"),
    ) {
        (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
        (None, None) => None,
        _ => {
            return Err(
                "TREMBITA_GATEWAY_TLS_CERT and TREMBITA_GATEWAY_TLS_KEY must both be set or both unset"
                    .into(),
            );
        }
    };
    let admin_tls = match (
        env("TREMBITA_ADMIN_TLS_CERT"),
        env("TREMBITA_ADMIN_TLS_KEY"),
    ) {
        (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
        (None, None) => None,
        _ => {
            return Err(
                "TREMBITA_ADMIN_TLS_CERT and TREMBITA_ADMIN_TLS_KEY must both be set or both unset"
                    .into(),
            );
        }
    };
    let drain_timeout = if env("TREMBITA_DRAIN_TIMEOUT").is_some() {
        env_overrides.drain_timeout = true;
        drain_timeout_from_env()
    } else {
        DEFAULT_DRAIN_TIMEOUT
    };
    let cert_watch = if env("TREMBITA_CERT_WATCH_SECS").is_some() {
        env_overrides.cert_watch = true;
        cert_watch_period_from_env()
    } else {
        Duration::from_secs(60)
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
        allow_voter_join,
        join_role,
        allow_leave,
        graceful_leave,
        voter_replacement,
        voter_replacement_grace_ticks,
        security,
        pem_paths,
        cert_dir,
        drain_timeout,
        cert_watch,
        data_dir,
        job_queue_stream,
        job_queue_lease,
        gateway,
        gateway_jobs_api,
        gateway_actors_api,
        gateway_workflows_api,
        gateway_introspect_api,
        gateway_drain_timeout: gateway_drain_timeout_from_env(),
        gateway_tls,
        env: env_overrides,
    })
}
