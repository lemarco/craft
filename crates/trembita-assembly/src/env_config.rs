//! Environment parsing shared by the `trembita` product facade and reference binaries.
//!
//! **Product surface** (typical `TrembitaApp::from_env` deploy):
//! `TREMBITA_LISTEN`, `TREMBITA_DATA_DIR`, `TREMBITA_CERT_DIR` (or `dev-certs`), `TREMBITA_JOIN_SEEDS`
//! on joiners, optional `GATEWAY_TOKEN` / `TREMBITA_JOB_QUEUE`.
//!
//! Full reference: [env.md](../../../docs/env.md).

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
#[allow(clippy::struct_excessive_bools)] // one flag per env toggle.
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
    /// `TREMBITA_VOTER_REPLACEMENT` was set.
    pub voter_replacement: bool,
    /// `TREMBITA_VOTER_REPLACEMENT_GRACE_TICKS` was set.
    pub voter_replacement_grace_ticks: bool,
    /// `TREMBITA_HTTP_TLS_*` was set.
    pub http_tls: bool,
}

/// Parsed product-app configuration from the environment.
#[allow(clippy::struct_excessive_bools)] // env toggles map 1:1 to optional features.
pub struct AppConfig {
    /// This node's id.
    pub node_id: NodeId,
    /// QUIC listen address (UDP wire).
    pub listen: SocketAddr,
    /// Product + ops HTTP listen address (TCP). Defaults to [`Self::listen`] when unset.
    pub http: Option<SocketAddr>,
    /// Optional HTTP TLS PEM paths (`TREMBITA_HTTP_TLS_*`).
    pub http_tls: Option<(PathBuf, PathBuf)>,
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
    /// Physical shards for [`Self::job_queue_stream`] (`TREMBITA_JOB_QUEUE_SHARDS`).
    pub job_queue_shards: Option<usize>,
    /// Adaptive shard growth for env-only job queue (`TREMBITA_JOB_QUEUE_AUTO_SHARD`).
    pub job_queue_auto_shard: bool,
    /// Multi-Raft coordination groups (`TREMBITA_RAFT_GROUPS`, default `1`).
    pub coordination_raft_groups: u32,
    /// Key routing shard count when multi-Raft is enabled (`TREMBITA_RAFT_SHARD_COUNT`).
    pub coordination_shard_count: Option<u32>,
    /// Parsed `TREMBITA_COORDINATION_PROFILE` (B-37).
    pub coordination_growth_profile: Option<crate::coordination_profile::CoordinationGrowthPreset>,
    /// HTTP connection drain timeout (`TREMBITA_HTTP_DRAIN_TIMEOUT`).
    pub http_drain_timeout: Duration,
    /// Explicit env vars that were set for this parse.
    pub env: EnvOverrides,
}

/// Alias for [`AppConfig`] in product apps and scaffold `config.rs`.
pub type ProductEnv = AppConfig;

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_bool(key: &str) -> bool {
    matches!(
        env(key).as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}

/// Product + ops TCP bind: **the same** `host:port` as QUIC wire (`wire` argument).
///
/// Wire and HTTP are different sockets (UDP vs TCP) on one port number — configured only
/// via [`AppConfig::listen`]. **`TREMBITA_HTTP` / `TREMBITA_GATEWAY` are internal:** set to `-` only to
/// skip the TCP listener (QUIC-only node). Product deploys should omit them.
///
/// # Errors
/// Returns an error when `TREMBITA_HTTP` / `TREMBITA_GATEWAY` is set to an address that
/// differs from `wire` (legacy split-port configs).
pub fn product_http_from_wire(wire: SocketAddr) -> Result<Option<SocketAddr>, Box<dyn Error>> {
    let raw = env("TREMBITA_HTTP").or_else(|| env("TREMBITA_GATEWAY"));
    match raw.as_deref() {
        None => Ok(Some(wire)),
        Some("-") => Ok(None),
        Some(addr) => {
            let parsed: SocketAddr = addr.parse()?;
            if parsed != wire {
                return Err(format!(
                    "TREMBITA_HTTP must use the same host:port as TREMBITA_LISTEN ({wire}); \
                     got {parsed}. Use only TREMBITA_LISTEN for the port, or TREMBITA_HTTP=- \
                     to disable TCP while keeping QUIC."
                )
                .into());
            }
            Ok(Some(wire))
        }
    }
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

fn http_drain_timeout_from_env() -> Duration {
    env("TREMBITA_HTTP_DRAIN_TIMEOUT")
        .or_else(|| env("TREMBITA_GATEWAY_DRAIN_TIMEOUT"))
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(crate::DEFAULT_GATEWAY_DRAIN_TIMEOUT, Duration::from_secs)
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
        .unwrap_or("0.0.0.0:443")
        .parse()?;
    let data_dir = env("TREMBITA_DATA_DIR").map(PathBuf::from);
    let http = product_http_from_wire(listen)?;
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
    let job_queue_shards = parse_job_queue_shards(env("TREMBITA_JOB_QUEUE_SHARDS"));
    let job_queue_auto_shard = env_bool("TREMBITA_JOB_QUEUE_AUTO_SHARD");
    validate_job_queue_scale_env(job_queue_shards, job_queue_auto_shard)?;
    let coordination_growth_profile = env("TREMBITA_COORDINATION_PROFILE")
        .as_deref()
        .and_then(crate::coordination_profile::parse_coordination_growth_profile);
    if env("TREMBITA_COORDINATION_PROFILE").is_some() && coordination_growth_profile.is_none() {
        return Err(
            "TREMBITA_COORDINATION_PROFILE must be one of: standard, jobs_backlog, write_sharding, full"
                .into(),
        );
    }
    let mut coordination_raft_groups = parse_coordination_raft_groups(env("TREMBITA_RAFT_GROUPS"));
    let mut coordination_shard_count =
        parse_coordination_shard_count(env("TREMBITA_RAFT_SHARD_COUNT"));
    let mut job_queue_auto_shard = job_queue_auto_shard;
    if let Some(profile) = coordination_growth_profile {
        let spec = profile.spec();
        if env("TREMBITA_RAFT_GROUPS").is_none() {
            coordination_raft_groups = spec.coordination_raft_groups;
        }
        if env("TREMBITA_RAFT_SHARD_COUNT").is_none() {
            coordination_shard_count = spec.coordination_shard_count;
        }
        if spec.env_job_queue_auto_shard
            && job_queue_stream.is_some()
            && job_queue_shards.is_none()
            && !env_bool("TREMBITA_JOB_QUEUE_AUTO_SHARD")
        {
            job_queue_auto_shard = true;
        }
    }
    validate_job_queue_scale_env(job_queue_shards, job_queue_auto_shard)?;
    let http_tls = match (
        env("TREMBITA_HTTP_TLS_CERT").or_else(|| env("TREMBITA_GATEWAY_TLS_CERT")),
        env("TREMBITA_HTTP_TLS_KEY").or_else(|| env("TREMBITA_GATEWAY_TLS_KEY")),
    ) {
        (Some(cert), Some(key)) => {
            env_overrides.http_tls = true;
            Some((PathBuf::from(cert), PathBuf::from(key)))
        }
        (None, None) => None,
        _ => {
            return Err(
                "TREMBITA_HTTP_TLS_CERT and TREMBITA_HTTP_TLS_KEY must both be set or both unset"
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

    log_non_product_env_warnings();

    Ok(AppConfig {
        node_id,
        listen,
        http,
        http_tls,
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
        job_queue_shards,
        job_queue_auto_shard,
        coordination_raft_groups,
        coordination_shard_count,
        coordination_growth_profile,
        http_drain_timeout: http_drain_timeout_from_env(),
        env: env_overrides,
    })
}

/// Warn once per boot when legacy / ops-only env vars are set ([env.md](../../../docs/env.md)).
pub fn log_non_product_env_warnings() {
    if env("TREMBITA_NODE_ID").is_some() {
        tracing::warn!(
            target: "trembita::env",
            "TREMBITA_NODE_ID is set — product apps persist id under TREMBITA_DATA_DIR/node-id; \
             use only for static clusters (trembita-node) or tests"
        );
    }
    if env("TREMBITA_PEERS").is_some() {
        tracing::warn!(
            target: "trembita::env",
            "TREMBITA_PEERS is set — static voter bootstrap; prefer TREMBITA_JOIN_SEEDS for elastic clusters"
        );
    }
    if env("TREMBITA_JOIN_SEEDS").is_some() && env("TREMBITA_PEERS").is_some() {
        tracing::warn!(
            target: "trembita::env",
            "both TREMBITA_JOIN_SEEDS and TREMBITA_PEERS are set — pick one bootstrap model"
        );
    }
    for key in ["TREMBITA_HTTP", "TREMBITA_GATEWAY"] {
        if let Some(raw) = env(key)
            && raw != "-"
        {
            tracing::warn!(
                target: "trembita::env",
                "{key}={raw} — prefer TREMBITA_LISTEN only (same port for wire + HTTP); \
                 use {key}=- for QUIC-only nodes"
            );
        }
    }
    let split_pem = env("TREMBITA_NODE_CERT").is_some()
        || env("TREMBITA_NODE_KEY").is_some()
        || env("TREMBITA_CA_CERT").is_some();
    if split_pem && env("TREMBITA_CERT_DIR").is_none() {
        tracing::warn!(
            target: "trembita::env",
            "TREMBITA_NODE_CERT/KEY/CA_CERT without TREMBITA_CERT_DIR — prefer cert dir + node-{{id}}.pem"
        );
    }
    for key in [
        "TREMBITA_GATEWAY_JOBS",
        "TREMBITA_GATEWAY_WORKFLOWS",
        "TREMBITA_GATEWAY_ACTORS",
        "TREMBITA_GATEWAY_INTROSPECT",
        "TREMBITA_ADMIN",
    ] {
        if env(key).is_some() {
            tracing::warn!(
                target: "trembita::env",
                "{key} is no longer read — use app registration / default gateway surfaces (0.5+)"
            );
        }
    }
}

/// Parse `TREMBITA_JOB_QUEUE_SHARDS` (`None` when unset or invalid).
#[must_use]
pub fn parse_job_queue_shards(raw: Option<String>) -> Option<usize> {
    raw.and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
}

/// Parse `TREMBITA_RAFT_GROUPS` (default `1`, minimum `1`).
#[must_use]
pub fn parse_coordination_raft_groups(raw: Option<String>) -> u32 {
    raw.and_then(|v| v.parse::<u32>().ok()).unwrap_or(1).max(1)
}

/// Parse `TREMBITA_RAFT_SHARD_COUNT` (`None` when unset or invalid).
#[must_use]
pub fn parse_coordination_shard_count(raw: Option<String>) -> Option<u32> {
    raw.and_then(|v| v.parse::<u32>().ok()).map(|n| n.max(1))
}

/// Reject env that sets both fixed shards and auto-shard (B-32).
///
/// # Errors
/// When both shard modes are enabled.
pub fn validate_job_queue_scale_env(
    shards: Option<usize>,
    auto_shard: bool,
) -> Result<(), Box<dyn Error>> {
    if shards.is_some() && auto_shard {
        return Err(
            "set either TREMBITA_JOB_QUEUE_SHARDS or TREMBITA_JOB_QUEUE_AUTO_SHARD=1, not both"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod b32_env_tests {
    use super::*;

    #[test]
    fn b32_parse_job_queue_shards_scenarios_table() {
        struct Row {
            raw: Option<&'static str>,
            want: Option<usize>,
        }
        let rows = [
            Row {
                raw: None,
                want: None,
            },
            Row {
                raw: Some("3"),
                want: Some(3),
            },
            Row {
                raw: Some("0"),
                want: None,
            },
            Row {
                raw: Some("nope"),
                want: None,
            },
        ];
        for row in rows {
            let got = parse_job_queue_shards(row.raw.map(str::to_string));
            assert_eq!(got, row.want, "raw={:?}", row.raw);
        }
    }

    #[test]
    fn b32_parse_coordination_raft_groups_scenarios_table() {
        struct Row {
            raw: Option<&'static str>,
            want: u32,
        }
        let rows = [
            Row { raw: None, want: 1 },
            Row {
                raw: Some("4"),
                want: 4,
            },
            Row {
                raw: Some("0"),
                want: 1,
            },
        ];
        for row in rows {
            let got = parse_coordination_raft_groups(row.raw.map(str::to_string));
            assert_eq!(got, row.want, "raw={:?}", row.raw);
        }
    }

    #[test]
    fn b32_parse_coordination_shard_count_scenarios_table() {
        struct Row {
            raw: Option<&'static str>,
            want: Option<u32>,
        }
        let rows = [
            Row {
                raw: None,
                want: None,
            },
            Row {
                raw: Some("64"),
                want: Some(64),
            },
            Row {
                raw: Some("0"),
                want: Some(1),
            },
        ];
        for row in rows {
            let got = parse_coordination_shard_count(row.raw.map(str::to_string));
            assert_eq!(got, row.want, "raw={:?}", row.raw);
        }
    }

    #[test]
    fn b37_profile_write_sharding_fills_raft_when_env_unset() {
        use crate::coordination_profile::CoordinationGrowthPreset;

        let profile = CoordinationGrowthPreset::WriteSharding;
        let spec = profile.spec();
        assert_eq!(spec.coordination_raft_groups, 2);
        assert_eq!(spec.coordination_shard_count, Some(64));
    }

    /// B-37 — env-only queue auto-shard is driven by [`CoordinationProfileSpec::env_job_queue_auto_shard`].
    #[test]
    fn b37_env_job_queue_auto_shard_from_profile_spec_table() {
        use crate::coordination_profile::CoordinationGrowthPreset;

        struct Row {
            preset: CoordinationGrowthPreset,
            want: bool,
        }
        let rows = [
            Row {
                preset: CoordinationGrowthPreset::Standard,
                want: false,
            },
            Row {
                preset: CoordinationGrowthPreset::JobsBacklog,
                want: true,
            },
            Row {
                preset: CoordinationGrowthPreset::WriteSharding,
                want: false,
            },
            Row {
                preset: CoordinationGrowthPreset::Full,
                want: true,
            },
        ];
        for row in rows {
            assert_eq!(
                row.preset.spec().env_job_queue_auto_shard,
                row.want,
                "{:?}",
                row.preset
            );
        }
    }

    #[test]
    fn b32_validate_job_queue_scale_env_scenarios_table() {
        struct Row {
            shards: Option<usize>,
            auto: bool,
            ok: bool,
        }
        let rows = [
            Row {
                shards: None,
                auto: false,
                ok: true,
            },
            Row {
                shards: Some(2),
                auto: false,
                ok: true,
            },
            Row {
                shards: None,
                auto: true,
                ok: true,
            },
            Row {
                shards: Some(2),
                auto: true,
                ok: false,
            },
        ];
        for row in rows {
            let result = validate_job_queue_scale_env(row.shards, row.auto);
            assert_eq!(
                result.is_ok(),
                row.ok,
                "shards={:?} auto={}",
                row.shards,
                row.auto
            );
        }
    }
}
