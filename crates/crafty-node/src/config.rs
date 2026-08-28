//! Environment-driven configuration for the `crafty-node` reference binary.

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;

use crafty::discovery::Seed;
use crafty::{NodeId, PeerDirectory, PemSecurity, Security, cert_paths_from_env};
use crafty_actor::DEFAULT_DRAIN_TIMEOUT;

/// Parsed runtime configuration from the environment.
pub struct NodeConfig {
    /// This node's id.
    pub node_id: NodeId,
    /// QUIC listen address.
    pub listen: SocketAddr,
    /// Admin HTTP listen address (`None` when disabled).
    pub admin: Option<SocketAddr>,
    /// Optional admin TLS PEM paths (`CRAFTY_ADMIN_TLS_CERT` + `CRAFTY_ADMIN_TLS_KEY`).
    pub admin_tls: Option<(PathBuf, PathBuf)>,
    /// Peer address book.
    pub peers: PeerDirectory,
    /// Static cluster members (may omit self when joining dynamically).
    pub members: Vec<NodeId>,
    /// Explicit join/discovery seeds.
    pub join_seeds: Vec<Seed>,
    /// A `dns:…` discovery spec resolved asynchronously in `main`.
    pub discovery: Option<DnsSpec>,
    /// Accept dynamic joins on this node.
    pub allow_join: bool,
    /// Accept cluster leave RPC on this node.
    pub allow_leave: bool,
    /// On shutdown, remove this node from the cluster via [`crafty::CraftyCluster::leave`]
    /// before stopping (requires at least one other live member).
    pub graceful_leave: bool,
    /// On-disk PEM paths when production/cert-manager material is configured.
    pub pem_paths: Option<crafty::CertPaths>,
    /// Loaded mTLS identity + trust roots.
    pub security: Security,
    /// Graceful actor drain timeout ([drain-timeout]).
    pub drain_timeout: Duration,
    /// Persistent data directory (`CRAFTY_DATA_DIR`); required when a job queue is enabled.
    pub data_dir: Option<PathBuf>,
    /// Job queue stream name (`CRAFTY_JOB_QUEUE`); enables leader queue service + redb backend.
    pub job_queue_stream: Option<String>,
    /// Lease visibility timeout for the job queue (`CRAFTY_JOB_QUEUE_LEASE_SECS`, default 60).
    pub job_queue_lease: Duration,
}

/// A parsed `CRAFTY_DISCOVERY=dns:<prefix>:<service>:<replicas>:<port>` spec.
pub struct DnsSpec {
    /// `StatefulSet` name prefix (e.g. `crafty`).
    pub prefix: String,
    /// Headless service name.
    pub service: String,
    /// Expected replica count.
    pub replicas: u64,
    /// QUIC port on each pod.
    pub port: u16,
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

/// Derive a node id: explicit `CRAFTY_NODE_ID` wins, otherwise a Kubernetes
/// `POD_NAME` ordinal (`crafty-0` → `NodeId(1)`), else `1`.
pub fn parse_node_id(
    crafty_node_id: Option<&str>,
    pod_name: Option<&str>,
) -> Result<NodeId, Box<dyn Error>> {
    if let Some(raw) = crafty_node_id {
        return Ok(NodeId(raw.parse()?));
    }
    if let Some(pod) = pod_name {
        let ordinal = parse_pod_ordinal(pod)?;
        return Ok(NodeId(ordinal + 1));
    }
    Ok(NodeId(1))
}

/// Derive a node id from the process environment.
pub fn node_id_from_env() -> Result<NodeId, Box<dyn Error>> {
    parse_node_id(env("CRAFTY_NODE_ID").as_deref(), env("POD_NAME").as_deref())
}

/// `StatefulSet` pod ordinal from a name (`crafty-2` → `2`).
pub fn parse_pod_ordinal(pod: &str) -> Result<u64, Box<dyn Error>> {
    pod.rsplit_once('-')
        .and_then(|(_, ord)| ord.parse().ok())
        .ok_or_else(|| format!("POD_NAME {pod:?} has no trailing ordinal (want name-N)").into())
}

/// `StatefulSet` pod ordinal from `POD_NAME` in the environment.
pub fn pod_ordinal_from_env() -> Result<u64, Box<dyn Error>> {
    let pod = env("POD_NAME")
        .ok_or("CRAFTY_CERT_ORDINAL_BASE requires POD_NAME (Kubernetes downward API)")?;
    parse_pod_ordinal(&pod)
}

/// PEM-related environment knobs for [`load_security`].
pub struct PemEnv<'a> {
    /// `CRAFTY_NODE_CERT`
    pub node_cert: Option<&'a str>,
    /// `CRAFTY_NODE_KEY`
    pub node_key: Option<&'a str>,
    /// `CRAFTY_CA_CERT`
    pub ca_cert: Option<&'a str>,
    /// `CRAFTY_CERT_ORDINAL_BASE`
    pub ordinal_base: Option<&'a str>,
    /// `POD_NAME` (required with `ordinal_base`)
    pub pod_name: Option<&'a str>,
}

/// Load mTLS material from explicit PEM paths or cert-manager ordinal mounts.
pub fn load_security(
    node_id: NodeId,
    members: &[NodeId],
    joining: bool,
    pem: &PemEnv<'_>,
) -> Result<(Security, Option<crafty::CertPaths>), Box<dyn Error>> {
    if let Some(base) = pem.ordinal_base {
        let ca = pem
            .ca_cert
            .ok_or("CRAFTY_CERT_ORDINAL_BASE requires CRAFTY_CA_CERT")?;
        let ordinal = match pem.pod_name {
            Some(pod) => parse_pod_ordinal(pod)?,
            None => pod_ordinal_from_env()?,
        };
        let paths = cert_paths_from_env(
            format!("{base}/{ordinal}/tls.crt"),
            format!("{base}/{ordinal}/tls.key"),
            ca,
        );
        let loaded = PemSecurity::load(node_id, paths.clone())?;
        return Ok((loaded.security, Some(paths)));
    }

    match (pem.node_cert, pem.node_key, pem.ca_cert) {
        (Some(cert), Some(key), Some(ca)) => {
            let paths = cert_paths_from_env(cert, key, ca);
            let loaded = PemSecurity::load(node_id, paths.clone())?;
            Ok((loaded.security, Some(paths)))
        }
        (None, None, None) => {
            if members.len() > 1 || joining {
                return Err("multi-node clusters need shared certs: set \
                     CRAFTY_NODE_CERT/CRAFTY_NODE_KEY/CRAFTY_CA_CERT on every node \
                     (mint them with examples/certs/generate.sh; see docs/certs.md), \
                     or CRAFTY_CERT_ORDINAL_BASE + CRAFTY_CA_CERT for cert-manager. \
                     A per-process dev CA only works for a single node."
                    .into());
            }
            let ca = crafty::net::tls::ClusterCa::generate()?;
            Ok((Security::dev(&ca, node_id)?, None))
        }
        _ => Err(
            "set all of CRAFTY_NODE_CERT, CRAFTY_NODE_KEY, CRAFTY_CA_CERT together, \
             or CRAFTY_CERT_ORDINAL_BASE + CRAFTY_CA_CERT + POD_NAME, or none for dev mode"
                .into(),
        ),
    }
}

/// Load mTLS material using the current process environment.
pub fn load_security_from_env(
    node_id: NodeId,
    members: &[NodeId],
    joining: bool,
) -> Result<(Security, Option<crafty::CertPaths>), Box<dyn Error>> {
    load_security(
        node_id,
        members,
        joining,
        &PemEnv {
            node_cert: env("CRAFTY_NODE_CERT").as_deref(),
            node_key: env("CRAFTY_NODE_KEY").as_deref(),
            ca_cert: env("CRAFTY_CA_CERT").as_deref(),
            ordinal_base: env("CRAFTY_CERT_ORDINAL_BASE").as_deref(),
            pod_name: env("POD_NAME").as_deref(),
        },
    )
}

/// Parse `CRAFTY_JOIN_SEEDS` (`id@host:port,...`) into a discovery seed set.
pub fn parse_seeds(raw: &str) -> Result<Vec<Seed>, Box<dyn Error>> {
    let mut seeds = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad CRAFTY_JOIN_SEEDS entry {entry:?} (want id@host:port)"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        seeds.push(Seed::new(NodeId(id), resolve_addr(addr)?));
    }
    Ok(seeds)
}

/// Parse `CRAFTY_DISCOVERY=dns:<prefix>:<service>:<replicas>:<port>`.
pub fn parse_discovery(raw: &str) -> Result<DnsSpec, Box<dyn Error>> {
    let parts: Vec<&str> = raw.split(':').collect();
    match parts.as_slice() {
        ["dns", prefix, service, replicas, port] => Ok(DnsSpec {
            prefix: (*prefix).to_string(),
            service: (*service).to_string(),
            replicas: replicas.parse()?,
            port: port.parse()?,
        }),
        _ => Err(format!(
            "bad CRAFTY_DISCOVERY {raw:?} (want dns:<prefix>:<service>:<replicas>:<port>)"
        )
        .into()),
    }
}

/// Resolve `host:port` to a `SocketAddr`, accepting both numeric IPs and DNS
/// names (e.g. docker-compose service names). Retries briefly so a peer whose
/// container is still coming up on the shared network doesn't fail the boot.
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

/// Parse `CRAFTY_PEERS` (`id@host:port,...`) into an address book + member list.
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

/// Load [`NodeConfig`] from the process environment.
///
/// # Errors
/// Returns an error when required variables are missing or invalid.
pub fn config_from_env() -> Result<NodeConfig, Box<dyn Error>> {
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
    let discovery = match env("CRAFTY_DISCOVERY") {
        Some(raw) => Some(parse_discovery(&raw)?),
        None => None,
    };
    let allow_join = env_bool("CRAFTY_ALLOW_JOIN");
    let allow_leave = env_bool("CRAFTY_ALLOW_LEAVE");
    let graceful_leave = env_bool("CRAFTY_GRACEFUL_LEAVE");
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

    let (mut peers, mut members) = match env("CRAFTY_PEERS") {
        Some(raw) => parse_peers(&raw)?,
        None => (PeerDirectory::new(), Vec::new()),
    };
    let joining = !join_seeds.is_empty() || discovery.is_some();
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
        return Err("CRAFTY_JOB_QUEUE requires CRAFTY_DATA_DIR for redb queue files".into());
    }
    let job_queue_lease = env("CRAFTY_JOB_QUEUE_LEASE_SECS")
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs);

    Ok(NodeConfig {
        node_id,
        listen,
        admin,
        admin_tls,
        peers,
        members,
        join_seeds,
        discovery,
        allow_join,
        allow_leave,
        graceful_leave,
        pem_paths,
        security,
        drain_timeout: drain_timeout_from_env(),
        data_dir,
        job_queue_stream,
        job_queue_lease,
    })
}

/// Parse `CRAFTY_DRAIN_TIMEOUT` (seconds, optional `s`/`m` suffix; default 60).
pub fn parse_drain_timeout(raw: Option<&str>) -> Duration {
    raw.and_then(parse_duration_token)
        .unwrap_or(DEFAULT_DRAIN_TIMEOUT)
}

fn parse_duration_token(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if let Some(stripped) = raw.strip_suffix('m') {
        stripped
            .parse::<u64>()
            .ok()
            .map(|m| Duration::from_secs(m.saturating_mul(60)))
    } else if let Some(stripped) = raw.strip_suffix('s') {
        stripped.parse::<u64>().ok().map(Duration::from_secs)
    } else {
        raw.parse::<u64>().ok().map(Duration::from_secs)
    }
}

/// Read [`parse_drain_timeout`] from `CRAFTY_DRAIN_TIMEOUT`.
pub fn drain_timeout_from_env() -> Duration {
    parse_drain_timeout(env("CRAFTY_DRAIN_TIMEOUT").as_deref())
}

/// Parse a cert-watch period (`CRAFTY_CERT_WATCH_SECS`, default 60).
pub fn parse_cert_watch_period(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_secs)
}

/// Parse `CRAFTY_CERT_WATCH_SECS` (default 60) for PEM hot reload polling.
pub fn cert_watch_period_from_env() -> Duration {
    parse_cert_watch_period(env("CRAFTY_CERT_WATCH_SECS").as_deref())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    const CRAFTY_ENV_KEYS: &[&str] = &[
        "CRAFTY_NODE_ID",
        "POD_NAME",
        "CRAFTY_LISTEN",
        "CRAFTY_ADMIN",
        "CRAFTY_ADMIN_TLS_CERT",
        "CRAFTY_ADMIN_TLS_KEY",
        "CRAFTY_PEERS",
        "CRAFTY_JOIN_SEEDS",
        "CRAFTY_DISCOVERY",
        "CRAFTY_ALLOW_JOIN",
        "CRAFTY_ALLOW_LEAVE",
        "CRAFTY_GRACEFUL_LEAVE",
        "CRAFTY_NODE_CERT",
        "CRAFTY_NODE_KEY",
        "CRAFTY_CA_CERT",
        "CRAFTY_CERT_ORDINAL_BASE",
        "CRAFTY_CERT_WATCH_SECS",
        "CRAFTY_DRAIN_TIMEOUT",
        "CRAFTY_DATA_DIR",
        "CRAFTY_JOB_QUEUE",
        "CRAFTY_JOB_QUEUE_LEASE_SECS",
    ];

    fn without_crafty_env(f: impl Fn() + std::panic::UnwindSafe + std::panic::RefUnwindSafe) {
        let clears = CRAFTY_ENV_KEYS
            .iter()
            .map(|key| (*key, None::<&str>))
            .collect::<Vec<_>>();
        temp_env::with_vars(clears, f);
    }

    fn with_crafty_env(
        vars: &[(&str, Option<&str>)],
        f: impl Fn() + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
    ) {
        let mut all = CRAFTY_ENV_KEYS
            .iter()
            .map(|key| (*key, None::<&str>))
            .collect::<Vec<_>>();
        all.extend_from_slice(vars);
        temp_env::with_vars(all, f);
    }

    fn generate_script() -> Option<PathBuf> {
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/certs/generate.sh");
        script.is_file().then_some(script)
    }

    fn run_generate(script: &Path, args: &[&str]) {
        let status = std::process::Command::new(script)
            .args(args)
            .status()
            .expect("run generate.sh");
        assert!(status.success(), "generate.sh failed: {args:?}");
    }

    #[test]
    fn explicit_pem_paths_load_for_single_node() {
        let Some(script) = generate_script() else {
            eprintln!("skip: generate.sh not found");
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let out = dir.path();
        run_generate(&script, &["--node-id", "1", "--out", out.to_str().unwrap()]);

        let node_cert = out.join("node-1.pem");
        let node_key = out.join("node-1.key");
        let ca_cert = out.join("ca.pem");
        with_crafty_env(
            &[
                ("CRAFTY_NODE_CERT", Some(node_cert.to_str().unwrap())),
                ("CRAFTY_NODE_KEY", Some(node_key.to_str().unwrap())),
                ("CRAFTY_CA_CERT", Some(ca_cert.to_str().unwrap())),
            ],
            || {
                let cfg = config_from_env().expect("config");
                assert_eq!(cfg.node_id, NodeId(1));
                assert!(cfg.pem_paths.is_some());
            },
        );
    }

    #[test]
    fn cert_manager_ordinal_base_resolves_pem_paths() {
        let Some(script) = generate_script() else {
            eprintln!("skip: generate.sh not found");
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let base = dir.path().join("certs");
        run_generate(
            &script,
            &["--node-id", "3", "--out", base.to_str().unwrap()],
        );
        let ordinal_dir = base.join("2");
        std::fs::create_dir_all(&ordinal_dir).expect("ordinal dir");
        std::fs::rename(base.join("node-3.pem"), ordinal_dir.join("tls.crt")).expect("crt");
        std::fs::rename(base.join("node-3.key"), ordinal_dir.join("tls.key")).expect("key");

        with_crafty_env(
            &[
                ("POD_NAME", Some("crafty-2")),
                ("CRAFTY_CERT_ORDINAL_BASE", Some(base.to_str().unwrap())),
                ("CRAFTY_CA_CERT", Some(base.join("ca.pem").to_str().unwrap())),
            ],
            || {
                let cfg = config_from_env().expect("config");
                assert_eq!(cfg.node_id, NodeId(3));
                let paths = cfg.pem_paths.expect("pem paths");
                assert!(paths.node_cert.ends_with("2/tls.crt"));
                assert!(paths.node_key.ends_with("2/tls.key"));
            },
        );
    }

    #[test]
    fn node_id_prefers_crafty_node_id_over_pod_name() {
        with_crafty_env(
            &[("CRAFTY_NODE_ID", Some("7")), ("POD_NAME", Some("crafty-2"))],
            || assert_eq!(node_id_from_env().unwrap(), NodeId(7)),
        );
    }

    #[test]
    fn node_id_derives_from_pod_name_ordinal() {
        with_crafty_env(&[("POD_NAME", Some("crafty-0"))], || {
            assert_eq!(node_id_from_env().unwrap(), NodeId(1));
        });
        with_crafty_env(&[("POD_NAME", Some("crafty-2"))], || {
            assert_eq!(node_id_from_env().unwrap(), NodeId(3));
        });
    }

    #[test]
    fn pod_ordinal_parses_trailing_segment() {
        with_crafty_env(&[("POD_NAME", Some("crafty-2"))], || {
            assert_eq!(pod_ordinal_from_env().unwrap(), 2);
        });
    }

    #[test]
    fn cert_watch_period_defaults_and_parses() {
        without_crafty_env(|| {
            assert_eq!(cert_watch_period_from_env(), Duration::from_secs(60));
        });
        with_crafty_env(&[("CRAFTY_CERT_WATCH_SECS", Some("15"))], || {
            assert_eq!(cert_watch_period_from_env(), Duration::from_secs(15));
        });
        assert_eq!(
            parse_cert_watch_period(Some("bad")),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn parse_peers_builds_member_list() {
        let (peers, members) =
            parse_peers("1@127.0.0.1:7443,2@127.0.0.1:7444").expect("parse peers");
        assert_eq!(members, vec![NodeId(1), NodeId(2)]);
        assert_eq!(peers.addr(NodeId(1)).unwrap().port(), 7443);
    }

    #[test]
    fn parse_discovery_parses_dns_spec() {
        let spec = parse_discovery("dns:crafty:crafty-headless:3:7443").expect("discovery");
        assert_eq!(spec.prefix, "crafty");
        assert_eq!(spec.service, "crafty-headless");
        assert_eq!(spec.replicas, 3);
        assert_eq!(spec.port, 7443);
    }

    #[test]
    fn parse_seeds_resolves_id_at_host_port() {
        let seeds = parse_seeds("2@127.0.0.1:7444").expect("seeds");
        assert_eq!(seeds.len(), 1);
        assert_eq!(seeds[0].node_id, NodeId(2));
        assert_eq!(seeds[0].addr.port(), 7444);
    }

    #[test]
    fn drain_timeout_defaults_and_parses() {
        assert_eq!(parse_drain_timeout(None), DEFAULT_DRAIN_TIMEOUT);
        assert_eq!(parse_drain_timeout(Some("90")), Duration::from_secs(90));
        assert_eq!(parse_drain_timeout(Some("2m")), Duration::from_secs(120));
    }

    #[test]
    fn config_from_env_honours_admin_disable_and_join_flags() {
        with_crafty_env(
            &[
                ("CRAFTY_ADMIN", Some("-")),
                ("CRAFTY_ALLOW_JOIN", Some("yes")),
                ("CRAFTY_ALLOW_LEAVE", Some("on")),
                ("CRAFTY_GRACEFUL_LEAVE", Some("true")),
            ],
            || {
                let cfg = config_from_env().expect("config");
                assert!(cfg.admin.is_none());
                assert!(cfg.allow_join);
                assert!(cfg.allow_leave);
                assert!(cfg.graceful_leave);
            },
        );
    }

    #[test]
    fn admin_tls_env_requires_both_cert_and_key() {
        with_crafty_env(&[("CRAFTY_ADMIN_TLS_CERT", Some("/tmp/cert.pem"))], || {
            assert!(config_from_env().is_err());
        });
    }

    #[test]
    fn job_queue_requires_data_dir() {
        with_crafty_env(&[("CRAFTY_JOB_QUEUE", Some("jobs"))], || {
            assert!(config_from_env().is_err());
        });
    }
}
